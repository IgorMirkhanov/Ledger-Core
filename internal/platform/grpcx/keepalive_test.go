package grpcx

import "testing"

// A client pinging more often than the server allows gets GOAWAY "too_many_pings"
// and loses in-flight RPCs. Verified manually against a real server (~30s to reproduce).
func TestServerToleratesClientKeepalive(t *testing.T) {
	if serverKeepaliveMinTime > ClientKeepaliveTime {
		t.Fatalf("server MinTime %s > client keepalive %s: server will drop client connections",
			serverKeepaliveMinTime, ClientKeepaliveTime)
	}
}
