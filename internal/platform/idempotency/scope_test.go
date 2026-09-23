package idempotency

import (
	"context"
	"testing"
)

func TestNamespaceSeparatesPrincipals(t *testing.T) {
	if Namespace("user-a", "1") == Namespace("user-b", "1") {
		t.Fatal("same key from different principals must not collide")
	}
	if got := Namespace("user-a", "1"); got != "user-a/1" {
		t.Fatalf("Namespace = %q", got)
	}
}

func TestReplayTracker(t *testing.T) {
	MarkReplayed(context.Background()) // no tracker: must not panic

	ctx, replayed := WithReplayTracker(context.Background())
	if replayed() {
		t.Fatal("fresh tracker reports replay")
	}
	MarkReplayed(ctx)
	if !replayed() {
		t.Fatal("MarkReplayed not recorded")
	}
}
