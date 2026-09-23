package gateway

import (
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/redis/go-redis/v9"
)

// Options configures the public API router.
type Options struct {
	API         *API
	Redis       *redis.Client
	RateLimit   int
	JWTSecret   []byte
	AppEnv      string
	UpstreamTTL time.Duration
}

// NewRouter builds the chi router with middleware in the documented order.
func NewRouter(opt Options) http.Handler {
	api := opt.API
	if api == nil {
		api = &API{}
	}
	api.Accounts, api.Transfers = WrapUpstreams(api.Accounts, api.Transfers)
	api.JWTSecret = opt.JWTSecret
	api.AppEnv = opt.AppEnv
	api.Upstream = opt.UpstreamTTL

	r := chi.NewRouter()
	r.Use(RequestIDMiddleware)
	r.Use(Recoverer)
	r.Use(AccessLog)
	r.Use(Auth(opt.JWTSecret, "/v1/dev/token"))
	if opt.Redis != nil {
		r.Use(RateLimit(opt.Redis, opt.RateLimit))
	}
	r.Use(RequireIdempotencyKey)
	r.Use(MaxBody(64 << 10))

	r.Route("/v1", func(r chi.Router) {
		r.Post("/dev/token", api.DevToken)
		r.Post("/accounts", api.CreateAccount)
		r.Get("/accounts", api.ListAccounts)
		r.Get("/accounts/{id}", api.GetAccount)
		r.Get("/accounts/{id}/statement", api.GetStatement)
		r.Post("/accounts/{id}/deposits", api.Deposit)
		r.Post("/accounts/{id}/withdrawals", api.Withdraw)
		r.Post("/transfers", api.CreateTransfer)
		r.Get("/transfers/{id}", api.GetTransfer)
		r.Get("/transfers", api.ListTransfers)
	})
	return r
}
