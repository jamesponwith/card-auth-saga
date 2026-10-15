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
| Increment X (limit-checked like a hold) | +X | | −X |
| Partial capture C ≤ open hold H | −C | +C | |
| Final capture C ≤ H | −H | +C | +(H−C) |
| Release (expiry, reversal, failed capture) | −H | | +H |

H is the open hold: A plus increments, minus partial captures.
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
auth ID). The update returns a `Decision` (`Approved`, plus a decline `Reason`) as soon as the hold is placed or declined; the workflow
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

## Incremental auth and multiple captures (M8)

Split shipments capture several times against one hold, and hotels and car rentals raise the hold
after the first auth.

- `Capture{Amount, Partial: true}` posts without closing the hold. A final capture (`Partial: false`,
  the old behaviour), a reversal or expiry closes it and releases whatever is left. If anything was
  captured, the auth ends `captured`.
- The `increment` update (`{ID, Amount}`) raises the hold while it's open. The limit check is the same
  atomic one a hold uses, and the update returns `false` if the card can't cover it.
- Rewards accrue once on the captured total when the hold closes, and the refund cap is that total.
- Workflows started before M8 keep single-capture behaviour. `workflow.GetVersion` tags new runs, and
  old runs reject partial captures and increments, so their histories still replay.

## Statement cycle (M9)

One `StatementWorkflow` per card (workflow ID `statement:<card>`) closes a statement every cycle
(30 days by default):

| Step | posted | open-to-buy |
|------|-------:|------------:|
| Payment P | −P | +P |
| Late fee F, or interest I | +F / +I | −F / −I |

- **Close:** the statement balance is the card's posted balance. The minimum due is the larger of
  $25 and 2% of the balance, but never more than the balance. Payment is due 25 days after close.
- **Late fee ($25):** charged at the due date if payments since close are below the minimum.
- **Interest:** charged at the next close on the average daily balance (M12).
- **Payments:** a `pay` update (`{ID, Amount}`). Each ID posts once in the ledger, ever. The posting
  also credits a memo account for that statement (`card:<id>:paid:s<N>`, balanced by
  `card:<id>:payments`), and the statement's paid total is read back from it. So a payment resent in a
  later cycle posts nothing and counts for nothing. While the statement
  is closing, `pay` is rejected so the caller retries against the next cycle.
- A `statement` query returns the current statement.
- After each close, **ContinueAsNew** carries only the new statement forward, so history stays bounded.

## Velocity check (M10)

Card-testing fraud hits a card with many rapid small auths. `PlaceHold` records each auth attempt
per card and declines one that makes **more than 5 attempts in 60 seconds**, before the limit check.
Declined attempts count too.

- Attempts are keyed by auth ID, and each is counted in the window ending at its *first* recorded
  time. A retried `PlaceHold` therefore gets the same answer, even if more attempts arrived since.
- No workflow code changes. The check happens inside an existing activity, so old histories replay
  untouched.
- Every decline carries a reason, in both the authorize `Decision` and `AuthResult.Reason`:
  `Velocity`, `OverLimit` or `UnknownCard`. `PlaceHold` reports velocity and unknown-card declines as
  non-retryable typed errors, and an over-limit decline as `false`. Old histories only hold
  `true`/`false` results, so they replay unchanged.

## Per-card config and migrations (M11)

Card limits and partner reward rates live in MySQL, not in code:

- `cards(id, credit_limit)` and `partners(merchant, points_per_dollar)`. `CHECK` constraints reject
  negative values. A card with no row is an `UnknownCard` decline, and a merchant with no row earns the
  default rate. A partner row with rate 0 earns no points.
- Activities read them through a `Config` interface. Tests and the in-memory worker use a static
  map; the MySQL worker uses the tables. Set them with `go run . set-limit <card> <cents>` and
  `go run . set-rate <merchant> <points-per-dollar>`.
- The limit is read just before the hold's ledger transaction. A limit change that races an auth
  applies one value or the other, never a mix. Balances are still checked under the row locks.
- The schema is versioned (ADR 0005). Migrations are ordered and recorded in `schema_migrations`,
  applied under an advisory lock, and each one is re-runnable. A database newer than the binary is
  refused.

## Statements v2 (M12)

- **Statements start with the card.** `set-limit` starts `statement:<card>` with conflict policy
  `USE_EXISTING`, so it creates the cycle once and leaves a running one alone. If Temporal is
  unreachable, the limit is still saved and the command says to run it again.
- **APR per card.** The rate lives in `cards.apr_bps` (default 2499, `CHECK >= 0`), is set with
  `set-apr`, and is read at each close, so a change applies from the next statement. Migration 4 adds
  the column. It's the first `ALTER`; on a re-run, MySQL's duplicate-column error counts as already
  applied (ADR 0005).
- **Average daily balance.** The cycle runs day by day. At the end of each day it samples the posted
  balance, counting a credit as 0. On the due day, the late fee posts before that day's sample.
  Interest = Σ daily balances × APR / 365. It's waived if the last statement was paid in full by the
  close, and the first cycle never carries interest.
- **Outages don't end billing.** The statement's own ledger steps retry until the ledger answers.
  A statement has nothing to compensate, so giving up would stop the card's billing for good.
- Runs started earlier stay on the close-timer path (`GetVersion("daily-balance-interest")`), and their
  recorded histories are kept so they keep replaying.

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
- **M8**: Incremental auth and multiple captures.
- **M9**: Monthly statement cycle (ContinueAsNew, query, late fee, interest).
- **M10**: Velocity check (more than 5 auths in 60s declines).
- **M11**: Per-card config tables and versioned migrations.
- **M12**: Statements start with the card; per-card APR; average-daily-balance interest.

## Success criteria

All met as of M5. Evidence in `workflow_test.go`, `ledger_test.go`, and `drill_test.go`:

- [x] Ledger sums to zero after every test and every drill.
- [x] A workflow survives a worker kill mid-hold and completes correctly on restart.
- [x] Duplicate `capture` signals post exactly once (and duplicate refund IDs).
- [x] Workflow replay test passes (determinism checked with the SDK replayer, against committed golden histories).
- [x] `go test -short ./...` stays under 10s; PR gate green.
