# 0006. Operate: health and metrics from the worker; probing waits for a deployment

Date: 2026-10-15
Status: accepted

## Context

The flywheel template added an Operate stage (template ADR 0003): `docs/slo.yml`, a `/healthz` contract,
and a prober that files `incident` issues for Learn's change-failure rate and MTTR. This repo is a
Temporal worker with no HTTP API and no deployment yet. The template's own history shows that probing an
address that isn't deployed files incidents that never close, and those inflate the DORA numbers.

## Decision

The worker serves the template's contract (`healthz.go`, byte-identical) on `CAS_HTTP_ADDR`. Health is
"ok" only if Temporal and MySQL both answer; otherwise it reports `degraded: <dependency>`. The Temporal
SDK's metrics go to `/debug/vars` through a stdlib `expvar` handler, so there's no metrics dependency.
`operate.yml` comes over manual-only until `docs/slo.yml` names a real deployment.

## Consequences

A deployment gets health, version attribution (goreleaser stamps `main.Version`) and SDK metrics as soon
as it exists, and turning on probing is a one-line schedule change. expvar has no histograms: timers are
a count and a total, so there are means but no percentiles. Switch to the SDK's tally or Prometheus
handler when percentiles matter. ADR numbers cited inside `healthz.go` refer to the
template's ADRs: the file stays byte-identical so syncs stay clean.
