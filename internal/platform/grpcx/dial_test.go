package grpcx_test

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/resolver/manual"

	"github.com/IgorMirkhanov/ledger-core/internal/platform/grpcx"
)

type countingHealth struct {
	healthpb.UnimplementedHealthServer
	n *atomic.Int64
}

func (c countingHealth) Check(context.Context, *healthpb.HealthCheckRequest) (*healthpb.HealthCheckResponse, error) {
	c.n.Add(1)
	return &healthpb.HealthCheckResponse{Status: healthpb.HealthCheckResponse_SERVING}, nil
}

// Dial must spread calls over all resolved backends (round_robin), not pin to one (pick_first).
func TestDialBalancesAcrossBackends(t *testing.T) {
	var counts [2]atomic.Int64
	addrs := make([]resolver.Address, 2)
	for i := range 2 {
		lis, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		s := grpc.NewServer()
		healthpb.RegisterHealthServer(s, countingHealth{n: &counts[i]})
		go func() { _ = s.Serve(lis) }()
		t.Cleanup(s.Stop)
		addrs[i] = resolver.Address{Addr: lis.Addr().String()}
	}
	r := manual.NewBuilderWithScheme("test")
	r.InitialState(resolver.State{Addresses: addrs})

	conn, err := grpcx.Dial("test:///backends", grpc.WithResolvers(r))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	client := healthpb.NewHealthClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for range 20 {
		if _, err := client.Check(ctx, &healthpb.HealthCheckRequest{}); err != nil {
			t.Fatal(err)
		}
	}
	if counts[0].Load() == 0 || counts[1].Load() == 0 {
		t.Fatalf("calls not balanced: backend0=%d backend1=%d", counts[0].Load(), counts[1].Load())
	}
}
