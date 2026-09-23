package idempotency

import (
	"context"
	"sync/atomic"
)

// Namespace binds a client-supplied key to the caller who sent it.
//
// idempotency_keys is keyed by (scope, key). Without a namespace, two users who pick the same key
// ("1", "retry-1", ...) collide: the second one gets IDEMPOTENCY_KEY_REUSED for a request it never made.
// Client keys are untrusted input, so they are always prefixed with the authenticated principal.
// Internal saga keys ("transfer:<id>:<step>") are already globally unique and use principal "svc:transfers".
func Namespace(principal, key string) string {
	return principal + "/" + key
}

type replayKey struct{}

// WithReplayTracker returns a ctx that records whether the use case served a stored response,
// and a func to read the result after the call. Transport installs it; service calls MarkReplayed.
// This keeps the replay signal out of use-case return types.
func WithReplayTracker(ctx context.Context) (context.Context, func() bool) {
	flag := new(atomic.Bool)
	return context.WithValue(ctx, replayKey{}, flag), flag.Load
}

// MarkReplayed records a replay in ctx; a no-op without a tracker.
func MarkReplayed(ctx context.Context) {
	if f, ok := ctx.Value(replayKey{}).(*atomic.Bool); ok {
		f.Store(true)
	}
}
