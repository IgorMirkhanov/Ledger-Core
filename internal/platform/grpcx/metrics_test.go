package grpcx

import "testing"

func TestServiceFromMethod(t *testing.T) {
	for in, want := range map[string]string{
		"/ledger.transfers.v1.TransfersService/CreateTransfer": "ledger.transfers.v1.TransfersService",
		"/grpc.health.v1.Health/Check":                         "grpc.health.v1.Health",
		"broken":                                               "unknown",
		"":                                                     "unknown",
	} {
		if got := serviceFromMethod(in); got != want {
			t.Errorf("serviceFromMethod(%q) = %q, want %q", in, got, want)
		}
	}
}
