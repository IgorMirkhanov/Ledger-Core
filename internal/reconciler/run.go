package reconciler

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/push"
)

// Result is the outcome of one reconciliation run.
type Result struct {
	RunID         uuid.UUID
	ChecksTotal   int
	Discrepancies []Discrepancy
	Status        string // ok | discrepancies | error
	StartedAt     time.Time
	FinishedAt    time.Time
}

// Run executes all checks in one REPEATABLE READ READ ONLY snapshot, then
// persists the report in a separate read-write transaction.
func Run(ctx context.Context, pool *pgxpool.Pool, checks []Check, log *slog.Logger) (*Result, error) {
	if len(checks) == 0 {
		checks = DefaultChecks()
	}
	if log == nil {
		log = slog.Default()
	}
	started := time.Now().UTC()
	runID := uuid.Must(uuid.NewV7())

	tx, err := pool.BeginTx(ctx, pgx.TxOptions{
		IsoLevel:   pgx.RepeatableRead,
		AccessMode: pgx.ReadOnly,
	})
	if err != nil {
		return nil, fmt.Errorf("reconciler: begin snapshot: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()

	var all []Discrepancy
	for _, c := range checks {
		found, err := c.Run(ctx, tx)
		if err != nil {
			_ = tx.Rollback(context.WithoutCancel(ctx))
			res := &Result{
				RunID: runID, ChecksTotal: len(checks), Status: "error",
				StartedAt: started, FinishedAt: time.Now().UTC(),
			}
			_ = persist(ctx, pool, res, nil)
			return res, fmt.Errorf("check %s: %w", c.Name(), err)
		}
		all = append(all, found...)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("reconciler: commit snapshot: %w", err)
	}

	finished := time.Now().UTC()
	status := "ok"
	if len(all) > 0 {
		status = "discrepancies"
	}
	res := &Result{
		RunID:         runID,
		ChecksTotal:   len(checks),
		Discrepancies: all,
		Status:        status,
		StartedAt:     started,
		FinishedAt:    finished,
	}
	if err := persist(ctx, pool, res, all); err != nil {
		return res, err
	}
	logReport(log, res)
	return res, nil
}

func persist(ctx context.Context, pool *pgxpool.Pool, res *Result, discs []Discrepancy) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("reconciler: begin write: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()

	_, err = tx.Exec(ctx, `
		INSERT INTO reconciliation_runs (id, started_at, finished_at, status, checks_total, discrepancies)
		VALUES ($1, $2, $3, $4, $5, $6)`,
		res.RunID, res.StartedAt, res.FinishedAt, res.Status, res.ChecksTotal, len(discs))
	if err != nil {
		return fmt.Errorf("reconciler: insert run: %w", err)
	}
	for _, d := range discs {
		_, err = tx.Exec(ctx, `
			INSERT INTO reconciliation_discrepancies (run_id, check_name, subject_id, expected, actual)
			VALUES ($1, $2, $3, $4, $5)`,
			res.RunID, d.CheckName, d.SubjectID, d.Expected, d.Actual)
		if err != nil {
			return fmt.Errorf("reconciler: insert discrepancy: %w", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("reconciler: commit write: %w", err)
	}
	return nil
}

func logReport(log *slog.Logger, res *Result) {
	log.Info("reconciliation finished",
		slog.String("run_id", res.RunID.String()),
		slog.String("status", res.Status),
		slog.Int("checks", res.ChecksTotal),
		slog.Int("discrepancies", len(res.Discrepancies)),
		slog.Duration("duration", res.FinishedAt.Sub(res.StartedAt)),
	)
	for _, d := range res.Discrepancies {
		log.Warn("discrepancy",
			slog.String("check", d.CheckName),
			slog.String("subject", d.SubjectID),
			slog.String("expected", d.Expected),
			slog.String("actual", d.Actual),
		)
	}
}

// PushMetrics pushes a summary gauge to Pushgateway when url is non-empty.
func PushMetrics(ctx context.Context, url, job string, res *Result) error {
	if url == "" {
		return nil
	}
	if job == "" {
		job = "reconciler"
	}
	g := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "ledger_reconciliation_discrepancies",
		Help: "Number of discrepancies found in the last reconciliation run.",
	})
	g.Set(float64(len(res.Discrepancies)))
	return push.New(url, job).Collector(g).Grouping("run_id", res.RunID.String()).Push()
}
