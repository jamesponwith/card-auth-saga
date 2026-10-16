package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"go.temporal.io/sdk/client"
)

// The ops endpoints: /healthz per the contract, and /debug/vars with the
// Temporal metrics map.
func TestOpsHandler(t *testing.T) {
	temporalMetrics.Add("ops_test_probe", 1)
	srv := httptest.NewServer(opsHandler(time.Now(), func() string { return "degraded: mysql: down" }, true))
	defer srv.Close()
	get := func(path string) (int, string) {
		t.Helper()
		res, err := http.Get(srv.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		defer res.Body.Close()
		body, _ := io.ReadAll(res.Body)
		return res.StatusCode, string(body)
	}

	code, body := get("/healthz")
	var h map[string]any
	if err := json.Unmarshal([]byte(body), &h); code != 200 || err != nil || h["status"] != "degraded: mysql: down" {
		t.Errorf("/healthz = %d %s (%v), want 200 with the status", code, body, err)
	}
	code, body = get("/debug/vars")
	var vars map[string]json.RawMessage
	if err := json.Unmarshal([]byte(body), &vars); code != 200 || err != nil || !strings.Contains(string(vars["temporal"]), "ops_test_probe") {
		t.Errorf("/debug/vars = %d, temporal map %s (%v), want the Temporal metrics", code, vars["temporal"], err)
	}
}

// With Temporal unreachable, health says so rather than "ok".
func TestWorkerHealthTemporalDown(t *testing.T) {
	c, err := client.NewLazyClient(client.Options{HostPort: "127.0.0.1:1"})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	pinged := false
	got := workerHealth(c, func(context.Context) error { pinged = true; return nil })()
	if !strings.HasPrefix(got, "degraded: temporal: ") {
		t.Errorf("health = %q, want degraded: temporal", got)
	}
	if pinged {
		t.Error("checked MySQL after Temporal failed; the first failure is the one to report")
	}
}

// With the metrics on a port of their own, the public handler serves /healthz
// and nothing else: a deployment can publish it without its internals.
func TestOpsHandlerWithoutDebug(t *testing.T) {
	public := httptest.NewServer(opsHandler(time.Now(), func() string { return "ok" }, false))
	defer public.Close()
	private := httptest.NewServer(debugHandler())
	defer private.Close()
	for _, tt := range []struct {
		port, url, path string
		want            int
	}{
		{"public", public.URL, "/healthz", http.StatusOK},
		{"public", public.URL, "/debug/vars", http.StatusNotFound},
		{"private", private.URL, "/debug/vars", http.StatusOK},
		{"private", private.URL, "/healthz", http.StatusNotFound},
	} {
		res, err := http.Get(tt.url + tt.path)
		if err != nil {
			t.Fatal(err)
		}
		_ = res.Body.Close()
		if res.StatusCode != tt.want {
			t.Errorf("%s on the %s port = %d, want %d", tt.path, tt.port, res.StatusCode, tt.want)
		}
	}
}
