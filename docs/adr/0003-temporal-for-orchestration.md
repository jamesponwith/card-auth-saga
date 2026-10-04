# 0003. Temporal for workflow orchestration

Date: 2026-10-04
Status: accepted

## Context

Card authorization is a long-running, multi-step money movement (hold → capture/expire → settle → refund) where every step must retry safely and survive process crashes. Hand-rolling that state machine (leases, timers, retries) is exactly what this project exists to replace — and learning Temporal is the point.

## Decision

Depend on the Temporal Go SDK (`go.temporal.io/sdk`) for workflows, signals, timers, and activity retries. Run the free local dev server (`temporal server start-dev`); no Temporal Cloud. MySQL driver gets its own ADR at M3.

## Consequences

Durable timers, retries, and crash recovery come for free; workflow code must stay deterministic (no wall clock, randomness, or I/O outside activities) — enforced by a replay test. Adds one heavyweight dependency, which the stdlib-first rule otherwise forbids.
