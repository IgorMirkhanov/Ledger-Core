// Package grpcx contains the gRPC server/client setup, interceptors, metadata helpers and error mapping.
package grpcx

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"runtime/debug"
	"time"

	"github.com/google/uuid"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/keepalive"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"

	"github.com/IgorMirkhanov/ledger-core/internal/platform/logger"
)

// Metadata keys propagated gateway → services → services.
const (
	MDIdempotencyKey = "idempotency-key"
	MDOwnerID        = "x-owner-id"
	MDRequestID      = "x-request-id"
	MDCaller         = "x-caller"
)

func firstMD(ctx context.Context, key string) string {
	md, _ := metadata.FromIncomingContext(ctx)
	if v := md.Get(key); len(v) > 0 {
		return v[0]
	}
	return ""
}

func IdempotencyKey(ctx context.Context) string { return firstMD(ctx, MDIdempotencyKey) }
func RequestID(ctx context.Context) string      { return firstMD(ctx, MDRequestID) }
func Caller(ctx context.Context) string         { return firstMD(ctx, MDCaller) }

// OwnerID returns the authenticated user id set by the gateway.
func OwnerID(ctx context.Context) (uuid.UUID, error) {
	v := firstMD(ctx, MDOwnerID)
	id, err := uuid.Parse(v)
	if err != nil {
		return uuid.Nil, status.Error(codes.Unauthenticated, "missing or invalid x-owner-id")
	}
	return id, nil
}

// Server wraps grpc.Server as an app.Runner.
type Server struct {
	srv    *grpc.Server
	health *health.Server
	addr   string
	log    *slog.Logger
}

// serverKeepaliveMinTime is the most frequent client ping the server tolerates.
// The gRPC default is 5 minutes: with ClientKeepaliveTime = 20s the server answers
// GOAWAY ENHANCE_YOUR_CALM "too_many_pings" after ~30s and drops the connection,
// failing in-flight RPCs with UNAVAILABLE. It must stay <= ClientKeepaliveTime.
const serverKeepaliveMinTime = 10 * time.Second

// NewServer creates a server with recovery + logging interceptors, health and reflection.
func NewServer(addr string, log *slog.Logger, extra ...grpc.ServerOption) *Server {
	opts := append([]grpc.ServerOption{
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             serverKeepaliveMinTime,
			PermitWithoutStream: true,
		}),
		grpc.ChainUnaryInterceptor(
			RecoveryInterceptor(log),
			LoggingInterceptor(log),
			// TODO(prompt-03): otelgrpc stats handler + prometheus interceptor.
		),
	}, extra...)
	s := grpc.NewServer(opts...)
	h := health.NewServer()
	healthpb.RegisterHealthServer(s, h)
	reflection.Register(s)
	return &Server{srv: s, health: h, addr: addr, log: log}
}

// Registrar exposes the underlying server for service registration.
func (s *Server) Registrar() grpc.ServiceRegistrar { return s.srv }

func (s *Server) Name() string { return "grpc-server" }

func (s *Server) Run(ctx context.Context) error {
	lis, err := (&net.ListenConfig{}).Listen(ctx, "tcp", s.addr)
	if err != nil {
		return fmt.Errorf("grpc listen %s: %w", s.addr, err)
	}
	errCh := make(chan error, 1)
	go func() { errCh <- s.srv.Serve(lis) }()
	s.log.Info("grpc server listening", slog.String("addr", s.addr))

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		s.health.Shutdown()
		stopped := make(chan struct{})
		go func() { s.srv.GracefulStop(); close(stopped) }()
		select {
		case <-stopped:
		case <-time.After(10 * time.Second):
			s.srv.Stop()
		}
		return nil
	}
}

// RecoveryInterceptor converts panics into codes.Internal.
func RecoveryInterceptor(log *slog.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
		defer func() {
			if p := recover(); p != nil {
				log.Error("panic in grpc handler",
					slog.String("method", info.FullMethod), slog.Any("panic", p), slog.String("stack", string(debug.Stack())))
				err = status.Error(codes.Internal, "internal error")
			}
		}()
		return handler(ctx, req)
	}
}

// LoggingInterceptor logs every call and puts a request-scoped logger into ctx.
func LoggingInterceptor(log *slog.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		start := time.Now()
		ctx = logger.With(ctx, slog.String("request_id", RequestID(ctx)), slog.String("method", info.FullMethod))
		resp, err := handler(ctx, req)
		code := status.Code(err)
		lvl := slog.LevelInfo
		if code == codes.Internal || code == codes.Unknown || code == codes.DataLoss {
			lvl = slog.LevelError
		}
		logger.FromContext(ctx).Log(ctx, lvl, "grpc call",
			slog.String("code", code.String()),
			slog.Duration("duration", time.Since(start)),
			slog.Any("error", err))
		return resp, err
	}
}

// ErrorMapping maps a domain sentinel error to a gRPC code. The reason is err.Error() of the sentinel.
type ErrorMapping map[error]codes.Code

// ToStatus converts err into a gRPC status with ErrorInfo{reason, domain}.
// Unknown errors become codes.Internal without leaking details.
func ToStatus(err error, domain string, m ErrorMapping) error {
	if err == nil {
		return nil
	}
	if _, ok := status.FromError(err); ok && !isWrapped(err) {
		return err
	}
	for sentinel, code := range m {
		if errors.Is(err, sentinel) {
			st := status.New(code, err.Error())
			if withInfo, e := st.WithDetails(&errdetails.ErrorInfo{Reason: sentinel.Error(), Domain: domain}); e == nil {
				return withInfo.Err()
			}
			return st.Err()
		}
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return status.Error(codes.DeadlineExceeded, "deadline exceeded")
	}
	if errors.Is(err, context.Canceled) {
		return status.Error(codes.Canceled, "canceled")
	}
	return status.Error(codes.Internal, "internal error")
}

func isWrapped(err error) bool { return errors.Unwrap(err) != nil }

// Reason extracts ErrorInfo.reason from a gRPC error (client side), or "".
func Reason(err error) string {
	st, ok := status.FromError(err)
	if !ok {
		return ""
	}
	for _, d := range st.Details() {
		if info, ok := d.(*errdetails.ErrorInfo); ok {
			return info.Reason
		}
	}
	return ""
}

// IsTransient reports whether a client-side gRPC error is worth retrying.
func IsTransient(err error) bool {
	switch status.Code(err) {
	case codes.Unavailable, codes.DeadlineExceeded, codes.ResourceExhausted, codes.Aborted:
		return true
	case codes.Internal, codes.Unknown:
		// Unknown outcome: safe to retry ONLY because every mutating call carries an idempotency key.
		return true
	}
	return false
}
