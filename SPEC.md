# card-auth-saga — Spec

**One-liner:** A Temporal-orchestrated card authorization → capture/reversal saga in Go, posting to a double-entry MySQL ledger with rewards accrual.

**Why:** Learn Temporal by building the shape of a modern card issuer's backend (authorization, ledger, rewards) — the same money-moving problems as Capital One card authorizations, but with durable workflows instead of hand-rolled leases and retries.

## Scope

1. **Authorization workflow** — check limit → place a hold (pending ledger entries) → wait for a `capture` signal or the hold-expiry timer → post or release.
2. **Double-entry ledger** — every money movement balances to zero; every activity carries an idempotency key so retries never double-post.
3. **Saga compensation** — failed capture, partial capture, and refunds post compensating entries instead of mutating history.
4. **Rewards accrual** — points accrue on settlement per a partner rule table, and are clawed back on refund.
5. **Failure drills** — kill the worker mid-hold, duplicate signals, activity timeouts; the ledger must stay balanced and the workflow must finish.

## Ledger accounts

Each card has three accounts; every posting sums to zero.

| Step | holds | posted | open-to-buy |
|------|------:|-------:|------------:|
| Hold A | +A | | −A |
| Capture C ≤ A | −A | +C | +(A−C) |
| Release (expiry, reversal, failed capture) | −A | | +A |
| Refund R ≤ captured − refunded | | −R | +R |

Rewards are points, not cents, and balance against the brand partner that funds them:

| Step | card points | partner points-issued |
|------|------------:|----------------------:|
| Settle C at rate r | +⌊C·r/100⌋ | −⌊C·r/100⌋ |
| Refund R (remaining L → L−R) | −(⌊L·r/100⌋ − ⌊(L−R)·r/100⌋) | +same |

The rate (points per $1, default 1) is read once at settlement and reused for every refund, and
clawback is computed from the remaining spend rather than the refund alone, so a full refund always
returns the card to exactly 0 points.

Limit check: `holds + posted + amount ≤ limit`, evaluated in the same transaction as the hold (row locks on the card's accounts), so concurrent auths can't oversubscribe a card.

## Non-goals

- Real card networks (ISO 8583), PCI scope, or real money.
- UI. A CLI and `temporal` web UI are enough.
- Multi-region / multi-cluster Temporal.
- Temporal Cloud — the local dev server (`temporal server start-dev`) is free and sufficient.

## Milestones

- **M1**: Temporal Go SDK wired up; authorization workflow skeleton tested with the SDK test suite (no server needed); in-memory ledger with a balance invariant test.
- **M2**: Hold → capture / expire using signals and timers; idempotent activities.
- **M3**: MySQL ledger (transactions + unique idempotency keys); reversals and refunds as compensations.
- **M4**: Rewards accrual and refund clawback.
- **M5**: Failure drills + writeup (README demo, blog post) → resume Open Source line.

## Success criteria

- Ledger sums to zero after every test and every drill.
- A workflow survives a worker kill mid-hold and completes correctly on restart.
- Duplicate `capture` signals post exactly once.
- Workflow replay test passes (determinism checked with the SDK replayer).
- `go test -short ./...` stays under 10s; PR gate green.
