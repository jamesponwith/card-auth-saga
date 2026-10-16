package main

import (
	"context"
	"expvar"
	"fmt"
	"net/http"
	"time"

	"go.temporal.io/sdk/client"
)

// temporalMetrics holds the Temporal SDK's metrics, served at /debug/vars.
var temporalMetrics = expvar.NewMap("temporal")

// opsHandler serves the Operate stage's endpoints: /healthz (the contract in
// healthz.go, which tools/watch probes) and /debug/vars (expvar, including the
// Temporal SDK's metrics).
func opsHandler(started time.Time, status func() string) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("GET /healthz", Healthz(started, status))
	mux.Handle("GET /debug/vars", expvar.Handler())
	return mux
}

// workerHealth reports "ok" when the worker can reach everything it needs and
// the latest scheduled audit was clean, or "degraded: <what>: <why>" for the
// first that isn't. ping checks the
// ledger's database; nil means the ledger is in memory.
func workerHealth(c client.Client, ping func(context.Context) error) func() string {
	return func() string {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if _, err := c.CheckHealth(ctx, &client.CheckHealthRequest{}); err != nil {
			return "degraded: temporal: " + err.Error()
		}
		if ping != nil {
			if err := ping(ctx); err != nil {
				return "degraded: mysql: " + err.Error()
			}
		}
		findings, err := lastAuditFindings(ctx, c)
		switch {
		case err != nil:
			return "degraded: audit: " + err.Error()
		case len(findings) > 0:
			return fmt.Sprintf("degraded: audit: %d findings, first %s %s: %s", len(findings), findings[0].Check, findings[0].Subject, findings[0].Detail)
		}
		return "ok"
	}
}
