package grpcx

import (
	"fmt"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
)

// ClientKeepaliveTime is how often clients ping an idle connection.
// Servers must allow it: see serverKeepaliveMinTime in grpcx.go.
const ClientKeepaliveTime = 20 * time.Second

// Dial opens a client connection with insecure transport and keepalive.
// addr may be "host:port" (wrapped as dns:///) or a full target URI.
func Dial(addr string, extra ...grpc.DialOption) (*grpc.ClientConn, error) {
	target := addr
	if !strings.Contains(addr, "://") {
		target = "dns:///" + addr
	}
	opts := append([]grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
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
