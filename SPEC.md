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

## Synchronous decision (M6)

A network needs approve/decline in ~100ms, but the workflow's result arrives days later. The caller
sends an `authorize` Update with **Update-with-Start** (conflict policy `USE_EXISTING`, workflow ID =
auth ID). The update returns `true`/`false` as soon as the hold is placed or declined; the workflow
carries on with the hold.

- A retried request with the same auth ID attaches to the running workflow and gets the same answer.
  No second hold is placed.
- If `PlaceHold` fails (bad amount), the update returns that error rather than hanging.
- The update has no arguments and no side effects, so old histories without it still replay.

## Capture, reverse, refund as updates (M7)

Signals are fire-and-forget: a bad capture or an over-refund is logged and dropped, and the merchant
never finds out. `capture`, `reverse` and `refund` are also Updates (same names as the signals):

- **Validators** reject a request before it is written to history: wrong phase (capture after
  release, refund before capture or after the window), bad amount, a refund ID already used, or a
  refund over what is left.
- The update **returns once the ledger has posted**, or with the error that stopped it.
- A network retry reuses the same **update ID**, so Temporal returns the first result instead of
  running the update again. A new update ID with a used refund ID is rejected.
- Concurrent requests can't double-capture or over-refund. A capture or release claims the hold
  before its first activity, and a refund reserves its amount before it posts.
- Signals keep working as before, so workflows already running on old code and the golden
  histories still replay.

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
- **M6**: Synchronous approve/decline via Update-with-Start.
- **M7**: Capture, reverse and refund as validated updates.

## Success criteria

All met as of M5. Evidence in `workflow_test.go`, `ledger_test.go`, and `drill_test.go`:

- [x] Ledger sums to zero after every test and every drill.
- [x] A workflow survives a worker kill mid-hold and completes correctly on restart.
- [x] Duplicate `capture` signals post exactly once (and duplicate refund IDs).
- [x] Workflow replay test passes (determinism checked with the SDK replayer, against committed golden histories).
- [x] `go test -short ./...` stays under 10s; PR gate green.
