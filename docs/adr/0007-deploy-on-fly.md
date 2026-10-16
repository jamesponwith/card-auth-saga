# 0007. Deploy to Fly.io on release tags, with a self-hosted Temporal

Date: 2026-10-16
Status: accepted

## Context

The Operate stage (ADR 0006) waits on a real deployment. The probe runs on GitHub's hosted runners, so the
worker's `/healthz` must be public; MySQL and Temporal must not be. The SPEC rules out Temporal Cloud, and
the official multi-service Temporal setup (frontend, history, matching and worker services, plus schema
jobs) is a lot to operate for one worker.

## Decision

Three Fly apps on Fly's private network: MySQL 8.4 on a volume; Temporal as the CLI's single-process
server (the same 1.9.1 the drills and load test run, pinned and checksummed), persisted to SQLite on a
volume; and the worker, which serves `/healthz` publicly and `/debug/vars` only on the private network. `release.yml` deploys on `v*` tags through
`deploy/fly/deploy.sh`, then smoke-tests that `/healthz` reports ok and the tag's version.

## Consequences

One machine each, about the cost of a coffee a month, and the deploy is a script anyone can read. A
SQLite-backed server has no replication: losing the Temporal volume loses workflow history, though not
the ledger. Move to the multi-service Temporal on MySQL when uptime or throughput needs it; the worker
only needs `TEMPORAL_ADDRESS` to change. A worker that fails the smoke test is rolled back to the image digest that was
running, and the job still fails. A release that migrated the schema can't be rolled back that way,
because the old worker refuses a newer schema (ADR 0005). The script says so; fix forward.
