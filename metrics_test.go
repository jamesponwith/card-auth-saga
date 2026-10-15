package main

import (
	"expvar"
	"sync"
	"testing"
	"time"
)

func metricValue(t *testing.T, m *expvar.Map, key string) string {
	t.Helper()
	v := m.Get(key)
	if v == nil {
		t.Fatalf("no metric %q in %s", key, m.String())
	}
	return v.String()
}

// The Temporal SDK's counters, gauges and timers land in an expvar map, one
// series per name and tag set.
func TestExpvarMetrics(t *testing.T) {
	m := new(expvar.Map).Init()
	h := expvarMetrics{m: m}
	worker := h.WithTags(map[string]string{"task_queue": "card-auth", "worker_type": "WorkflowWorker"})

	h.Counter("temporal_request").Inc(2)
	h.Counter("temporal_request").Inc(3)
	worker.Counter("temporal_request").Inc(1)
	worker.WithTags(map[string]string{"worker_type": "ActivityWorker"}).Counter("temporal_request").Inc(7) // a tag overridden
	worker.Gauge("temporal_worker_task_slots_available").Update(10)
	worker.Gauge("temporal_worker_task_slots_available").Update(4)
	h.Timer("temporal_request_latency").Record(1500 * time.Millisecond)
	h.Timer("temporal_request_latency").Record(500 * time.Millisecond)

	tests := []struct{ key, want string }{
		{"temporal_request", "5"},
		{"temporal_request{task_queue=card-auth,worker_type=WorkflowWorker}", "1"},
		{"temporal_request{task_queue=card-auth,worker_type=ActivityWorker}", "7"},
		{"temporal_worker_task_slots_available{task_queue=card-auth,worker_type=WorkflowWorker}", "4"},
		{"temporal_request_latency_count", "2"},
		{"temporal_request_latency_seconds", "2"},
	}
	for _, tt := range tests {
		if got := metricValue(t, m, tt.key); got != tt.want {
			t.Errorf("%s = %s, want %s", tt.key, got, tt.want)
		}
	}
}

// Workers record from many goroutines at once.
func TestExpvarMetricsConcurrent(t *testing.T) {
	m := new(expvar.Map).Init()
	h := expvarMetrics{m: m}.WithTags(map[string]string{"q": "a"})
	var wg sync.WaitGroup
	for range 50 {
		wg.Go(func() {
			for range 100 {
				h.Counter("c").Inc(1)
				h.Gauge("g").Update(1)
				h.Timer("t").Record(time.Millisecond)
			}
		})
	}
	wg.Wait()
	if got := metricValue(t, m, "c{q=a}"); got != "5000" {
		t.Errorf("counter = %s, want 5000", got)
	}
	if got := metricValue(t, m, "t_count{q=a}"); got != "5000" {
		t.Errorf("timer count = %s, want 5000", got)
	}
}
