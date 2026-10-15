package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestHealthz(t *testing.T) {
	tests := []struct {
		name       string
		status     func() string
		wantStatus string
	}{
		{"default is ok", nil, "ok"},
		{"healthy", func() string { return "ok" }, "ok"},
		{"impaired reports degraded, still 200", func() string { return "degraded" }, "degraded"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			Healthz(time.Now().Add(-90*time.Second), tt.status)(rec, httptest.NewRequest("GET", "/healthz", nil))

			if rec.Code != http.StatusOK {
				t.Fatalf("code = %d, want 200 — a 5xx is indistinguishable from being down", rec.Code)
			}
			var got map[string]any
			if err := json.NewDecoder(rec.Body).Decode(&got); err != nil {
				t.Fatalf("body is not JSON, which tools/watch treats as a breach: %v", err)
			}
			if got["status"] != tt.wantStatus {
				t.Errorf("status = %v, want %v", got["status"], tt.wantStatus)
			}
			if got["version"] != Version {
				t.Errorf("version = %v, want %v — Learn needs it to attribute incidents", got["version"], Version)
			}
			if uptime, ok := got["uptime_s"].(float64); !ok || uptime < 89 {
				t.Errorf("uptime_s = %v, want ~90", got["uptime_s"])
			}
		})
	}
}
