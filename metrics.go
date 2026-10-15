package main

import (
	"expvar"
	"maps"
	"slices"
	"strings"
	"time"

	"go.temporal.io/sdk/client"
)

// expvarMetrics exports the Temporal SDK's metrics through expvar, so the
// worker serves them at /debug/vars with no metrics dependency. Each series is
// keyed by name and sorted tags, name{k=v,...}; a timer is a _count and a
// _seconds total, enough for a rate and a mean.
type expvarMetrics struct {
	m    *expvar.Map
	tags map[string]string
}

func (h expvarMetrics) WithTags(tags map[string]string) client.MetricsHandler {
	merged := map[string]string{}
	maps.Copy(merged, h.tags)
	maps.Copy(merged, tags)
	return expvarMetrics{h.m, merged}
}

// key is name with its tags: name{a=1,b=2}, or just name with none.
func (h expvarMetrics) key(name, suffix string) string {
	if len(h.tags) == 0 {
		return name + suffix
	}
	var b strings.Builder
	b.WriteString(name + suffix + "{")
	for i, k := range slices.Sorted(maps.Keys(h.tags)) {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(k + "=" + h.tags[k])
	}
	b.WriteByte('}')
	return b.String()
}

type expvarCounter func(int64)
type expvarGauge func(float64)
type expvarTimer func(time.Duration)

func (f expvarCounter) Inc(d int64)          { f(d) }
func (f expvarGauge) Update(v float64)       { f(v) }
func (f expvarTimer) Record(d time.Duration) { f(d) }

func (h expvarMetrics) Counter(name string) client.MetricsCounter {
	key := h.key(name, "")
	return expvarCounter(func(d int64) { h.m.Add(key, d) })
}

func (h expvarMetrics) Gauge(name string) client.MetricsGauge {
	key := h.key(name, "")
	return expvarGauge(func(v float64) {
		h.m.AddFloat(key, 0) // creates the series once, safely
		h.m.Get(key).(*expvar.Float).Set(v)
	})
}

func (h expvarMetrics) Timer(name string) client.MetricsTimer {
	count, total := h.key(name, "_count"), h.key(name, "_seconds")
	return expvarTimer(func(d time.Duration) {
		h.m.Add(count, 1)
		h.m.AddFloat(total, d.Seconds())
	})
}
