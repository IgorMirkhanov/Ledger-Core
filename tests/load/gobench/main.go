// Command gobench is a closed-loop load generator for POST /v1/transfers.
//
// Unlike the k6 open-model scenarios it keeps a fixed number of requests in flight,
// which separates service latency (-c 1) from saturation behaviour (-c 25..100).
//
//	go run ./tests/load/gobench -base http://localhost:8080/v1 -users 100 -c 25 -d 30s
//
// Requires APP_ENV=local (dev tokens) and rate limits disabled (docker-compose.load.yml).
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand/v2"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type client struct {
	base string
	http *http.Client
	seq  atomic.Int64
}

// post sends a JSON POST with a fresh Idempotency-Key.
func (c *client) post(ctx context.Context, path, token string, body any) (int, map[string]any, error) {
	var r io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return 0, nil, err
		}
		r = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.base+path, r)
	if err != nil {
		return 0, nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	req.Header.Set("Idempotency-Key", fmt.Sprintf("gobench-%d-%d", time.Now().UnixNano(), c.seq.Add(1)))
	resp, err := c.http.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	var m map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&m)
	return resp.StatusCode, m, nil
}

type user struct{ token, a, b string }

func (c *client) setupUser(ctx context.Context) (user, error) {
	_, t, err := c.post(ctx, "/dev/token", "", map[string]any{})
	if err != nil {
		return user{}, err
	}
	token, _ := t["token"].(string)
	if token == "" {
		return user{}, fmt.Errorf("dev token: %v", t)
	}
	ids := make([]string, 2)
	for i := range ids {
		_, acc, err := c.post(ctx, "/accounts", token, map[string]any{"currency": "RUB"})
		if err != nil {
			return user{}, err
		}
		id, _ := acc["id"].(string)
		if id == "" {
			return user{}, fmt.Errorf("create account: %v", acc)
		}
		code, _, err := c.post(ctx, "/accounts/"+id+"/deposits", token,
			map[string]any{"amount": "100000000", "currency": "RUB"})
		if err != nil || code != http.StatusCreated {
			return user{}, fmt.Errorf("deposit: code=%d err=%w", code, err)
		}
		ids[i] = id
	}
	return user{token: token, a: ids[0], b: ids[1]}, nil
}

func main() {
	base := flag.String("base", "http://localhost:8080/v1", "gateway base URL")
	users := flag.Int("users", 100, "users, each with two funded RUB accounts")
	conc := flag.Int("c", 25, "requests in flight")
	dur := flag.Duration("d", 30*time.Second, "measurement duration")
	flag.Parse()

	c := &client{base: *base, http: &http.Client{
		Transport: &http.Transport{MaxIdleConnsPerHost: 1024},
		Timeout:   30 * time.Second,
	}}
	ctx := context.Background()

	us := make([]user, *users)
	var wg sync.WaitGroup
	errs := make(chan error, *users)
	for i := range us {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			u, err := c.setupUser(ctx)
			if err != nil {
				errs <- err
				return
			}
			us[i] = u
		}(i)
	}
	wg.Wait()
	close(errs)
	if err := <-errs; err != nil {
		log.Fatalf("setup: %v", err)
	}

	var (
		mu    sync.Mutex
		lat   []time.Duration
		codes = map[string]int{}
	)
	deadline := time.Now().Add(*dur)
	for range *conc {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for time.Now().Before(deadline) {
				u := us[rand.IntN(len(us))] //nolint:gosec // load pattern, not security
				src, dst := u.a, u.b
				if rand.IntN(2) == 0 { //nolint:gosec // load pattern, not security
					src, dst = dst, src
				}
				start := time.Now()
				code, m, err := c.post(ctx, "/transfers", u.token, map[string]any{
					"source_account_id": src, "dest_account_id": dst, "amount": "100", "currency": "RUB",
				})
				elapsed := time.Since(start)
				key := fmt.Sprint(code)
				if err != nil {
					key = "error"
				} else if s, ok := m["status"].(string); ok {
					key += "/" + s
				}
				mu.Lock()
				lat = append(lat, elapsed)
				codes[key]++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	if len(lat) == 0 {
		log.Fatal("no requests completed")
	}
	sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
	q := func(p float64) time.Duration { return lat[int(float64(len(lat)-1)*p)].Round(100 * time.Microsecond) }
	fmt.Printf("in_flight=%d requests=%d rps=%.0f p50=%v p95=%v p99=%v results=%v\n",
		*conc, len(lat), float64(len(lat))/dur.Seconds(), q(.50), q(.95), q(.99), codes)
}
