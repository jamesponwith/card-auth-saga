// The Operate stage's half of the contract (ADR 0002). Keep this when you
// replace main.go: tools/watch probes it, and the version it reports is what
// lets the Learn stage attribute an incident to a release.
package main

import (
	"encoding/json"
	"net/http"
	"time"
)

// Version is set at build time: -ldflags "-X main.Version=v0.1.0".
// goreleaser is already configured to do this in .goreleaser.yaml.
var Version = "dev"

// Healthz serves the contract tools/watch expects: 200 with
// {"status":"ok","version":...,"uptime_s":...}. Anything else — a non-200, a
// status other than "ok", or a body that is not JSON — is a breach.
//
// Report "degraded" rather than failing outright when the service is up but
// impaired; watch treats it as a breach either way, but the incident says why.
func Healthz(started time.Time, status func() string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		s := "ok"
		if status != nil {
			s = status()
		}
		w.Header().Set("Content-Type", "application/json")
		if s != "ok" {
			// Still 200: the body carries the detail, and a 5xx here would be
			// indistinguishable from the process being down.
			w.WriteHeader(http.StatusOK)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"status":   s,
			"version":  Version,
			"uptime_s": int(time.Since(started).Seconds()),
		})
	}
}
