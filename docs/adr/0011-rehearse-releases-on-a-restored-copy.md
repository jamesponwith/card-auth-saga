# 0011. Rehearse a release on a restored copy of production

Date: 2026-10-23
Status: accepted

## Context

A release can break on what only production has: its data (a migration meets real rows) and its
running workflows (their histories replay on the new code). Tests and drills run on fresh databases
and short-lived workflows, and the restore drill boots the copy on production's *current* images,
so none of them prove an upgrade. Production's MySQL password lives only in GitHub Actions secrets.

## Decision

`UPGRADE=<version>` extends the restore drill. After the restore checks, it builds this checkout as a
release and runs it as a worker on the copy's Temporal and MySQL. Then it checks: health at the
version; the schema migrated; every card has its number; every running workflow replays (a stack
query replays its whole history); the audit is clean; and a replacement, authorize and subscribe round
trip. Dispatched from Actions: `gh workflow run restore-drill -f upgrade=true`.

## Consequences

Before a tag touches production, the release has run against production's data and in-flight
workflows. The copy is a snapshot, up to a day old, and it gets no production traffic, so it can't
catch load-dependent breaks (the load drill covers those). It costs about 15 minutes and a few cents.
