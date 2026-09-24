package grpcx

import (
	"fmt"
	"strings"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
)

// ClientKeepaliveTime is how often clients ping an idle connection.
// Servers must allow it: see serverKeepaliveMinTime in grpcx.go.
const ClientKeepaliveTime = 20 * time.Second

// roundRobin spreads RPCs over every address the resolver returns. With the default
// pick_first a client pins one long-lived connection to a single pod, so extra replicas
// behind a Kubernetes Service receive no traffic. Pair it with a headless Service
// (dns:///svc-headless:9090) so DNS returns every pod.
const roundRobin = `{"loadBalancingConfig":[{"round_robin":{}}]}`

// Dial opens a client connection with insecure transport and keepalive.
// addr may be "host:port" (wrapped as dns:///) or a full target URI.
func Dial(addr string, extra ...grpc.DialOption) (*grpc.ClientConn, error) {
	target := addr
	if !strings.Contains(addr, "://") {
		target = "dns:///" + addr
	}
	opts := append([]grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultServiceConfig(roundRobin),
		grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                ClientKeepaliveTime,
			Timeout:             10 * time.Second,
			PermitWithoutStream: true,
		}),
	}, extra...)
	conn, err := grpc.NewClient(target, opts...)
	if err != nil {
		return nil, fmt.Errorf("grpc dial %s: %w", addr, err)
	}
	return conn, nil
}
