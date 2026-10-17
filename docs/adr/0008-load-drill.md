# 0008. Load-test a throwaway copy of production, not production

Date: 2026-10-17
Status: accepted

## Context

TestLoad measures a laptop: a dev server and an in-process worker. What matters is the Fly stack
(ADR 0007): a 512MB worker, single-process Temporal on SQLite, MySQL, over the private network.
Loading production itself would put hundreds of load cards and auths in the real ledger forever,
since cards can't be deleted and the audit reads every one.

## Decision

`deploy/fly/load-drill.sh` stands up a `-drill-load-` copy of the stack with deploy.sh (same
recipes, machine sizes and region, fresh volumes, a password of its own), runs TestLoad against it
through `flyctl proxy`, and destroys it, like the restore drill. Run by hand, with an org token.

## Consequences

The numbers are production's hardware with an empty ledger, which flatters it slightly. Each latency
includes the round trip from wherever the drill runs to iad, so TestLoad logs that first. A copy
costs a few cents and ~10 minutes, so it's run before scaling decisions, not on a schedule.
