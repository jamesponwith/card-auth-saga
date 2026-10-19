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
- `PlaceHold`, the one step the decision waits on, runs as a local activity in the workflow worker
  (`GetVersion("local-place-hold")`; earlier runs replay on the remote activity).

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
- A card's attempts are recorded one at a time, under a lock row (`attempt_locks`, migration 6). Each
  is recorded at the later of its clock and just after the card's previous attempt, so arrival order is
  time order: racing attempts are counted exactly, and a burst can never slip past the cap.
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

## Disputes (M13)

A cardholder can dispute part or all of a captured charge while refunds are open. The dispute runs
inside the auth's own workflow, so refunds and disputes share one cap: together they never credit
more than was captured.

| Step | posted | open-to-buy |
|------|-------:|------------:|
| Provisional credit D, on opening | −D | +D |
| Cardholder wins | (the credit stands) | |
| Merchant wins: rebill D | +D | −D |

1. **Open:** the `dispute` update (`{ID, Amount}`) is validated against what's left after refunds and
   other disputes. It posts the provisional credit, so the statement's daily balances and interest
   stop counting the disputed amount right away, and it returns.
2. **Merchant response** (default 45 days): the `merchant-response` update (`{DisputeID, Accept}`).
   Accepting, or letting the deadline pass, means the cardholder wins. Contesting (`Accept: false`)
   sends the dispute to review.
3. **Review** (default 30 days): the `resolve-dispute` update (`{DisputeID, CardholderWins}`) records the
   analyst's ruling. A ruling that doesn't arrive by the deadline goes to the cardholder.
4. **Outcome:** if the cardholder wins, the dispute's points are clawed back like a refund's. If the
   merchant wins, the credit is rebilled and the points stay.

- Each dispute claims its money share when it opens. Its place in the points clawback chain is
  claimed when it's won, so the clawbacks always telescope to exact points (SPEC rewards), even when
  a lost dispute sits between refunds.
- A lost dispute keeps its share: the cardholder can't re-dispute or refund the same amount.
- The auth workflow stays open until every dispute resolves. `AuthResult.ChargedBack` is the total
  won. `Status` is `refunded` once refunds plus chargebacks equal the capture.
- The `disputes` query lists each dispute and its stage. Money steps retry until the ledger answers,
  because a provisional credit or rebill has nothing to compensate with.

## Ledger auditor (M15)

`AuditWorkflow` runs on a Temporal Schedule (`ledger-audit`, every 24h, overlapping runs skipped), and
on demand with `go run . audit`. It reports findings rather than fixing anything.

| Check | Rule |
|---|---|
| Card money | holds + posted + open-to-buy nets to 0, per card |
| Holds | never negative, per card |
| Points | a card's points plus those it redeemed are never negative (a refund after a redemption can take points below 0, never past what was earned); card points + redeemed + partner points-issued net to 0 |
| Payment memos | each card's `paid:s<N>` accounts + `payments` net to 0 |
| Whole ledger | every account sums to 0 |
| Stuck auths | no `AuthorizeWorkflow` running past its longest life (hold + refunds + both dispute deadlines: 120 days) |
| Stuck statements | no `StatementWorkflow` run older than two cycles (60 days) |
| Failures | no workflow failed or timed out since the previous audit (the last 24h) |
| Orphaned holds | no card holds money while no auth runs on it (checked twice, 2s apart, so an auth settling mid-audit isn't reported) |

- The workflow's result is the report, and each finding is logged. While the most recent scheduled
  audit has findings, `/healthz` reports `degraded: audit: <n> findings`, so the Operate prober files
  the incident with no new integration.
- The worker creates the schedule on boot if it's missing, and leaves an existing one alone.
- Account IDs are `card:<id>:<kind>` and `partner:<merchant>:points-issued`, and the audit quotes card
  IDs in visibility queries. So card IDs can't contain `:`, quotes or `\`, and `set-limit` rejects them.
- Each `AuthorizeWorkflow` tags itself with a `CardID` search attribute
  (`GetVersion("card-id-attribute")`), which the worker registers on boot. Auths started before
  tagging can't be attributed to a card, so while any is running the orphan check stands down.

## Freezing and closing cards (M16)

`cards.status` is `active`, `frozen` or `closed` (migration 5, `CHECK`), set with
`go run . freeze|unfreeze|close <card>`.

- **Frozen:** new auths decline with `Reason: Frozen`, and increments decline. Holds already placed
  carry on: captures, refunds and disputes in progress all finish, because freezing stops new credit,
  not existing obligations. `unfreeze` lifts it.
- **Closed:** like frozen, with `Reason: Closed`, and permanent. Freeze, unfreeze and `set-limit` on a
  closed card are refused, so a closed card can't be reopened by accident. The statement cycle keeps
  billing until a close finds nothing posted, nothing held and no auth still running on the card, then
  ends instead of continuing as new. Waiting for the last auth means a refund or chargeback can't land
  after the final statement.
- `PlaceHold` reads the status with the limit, so a status change applies to the next auth. Runs
  started before this replay unchanged: an old `PlaceHold` result is only `true`/`false`, and statement
  runs gate the new close step with `GetVersion("end-when-closed")`.

## Card replacement (M18)

A lost or stolen card gets a new number on the same account (ADR 0009):
`go run . replace <card> <new-number>`.

- **One account, many numbers.** The ID a card was created with is its account ID: its ledger
  accounts, `cards` row, `statement:<id>` and `CardID` attribute keep using it. `card_numbers`
  (migration 7, backfilled with each card's own ID) maps each number to its account. Replacing marks
  the old number `replaced_at` and adds the new one. Its primary key means a number can belong to
  only one account, ever.
- **Auths.** `ResolveCard` is a local activity that runs before `PlaceHold`. It maps the number to its
  account. A replaced number declines with `Reason: Replaced` and no attempt recorded, so a thief
  trying the old number can't trip the new card's velocity check. Otherwise the auth puts the account
  in `AuthRequest.CardID` and tags itself with it, so holds, captures, refunds, disputes, rewards,
  velocity and the audit all see one card. A number with no row is its own account, so an unknown
  number still declines `UnknownCard`.
- **Auths already running** on the old number carry on: captures, refunds and disputes settle on the
  account as usual.
- **Admin commands** (`set-limit`, `set-apr`, `freeze`, `unfreeze`, `close`, `replace`) accept any of the
  card's numbers and act on its account. `set-limit` with a new ID creates the card and its number,
  and refuses an ID that is already another card's number.
- **Refused:** replacing a number that was already replaced, a closed card, an unknown card, or
  replacing with a number already in use. The new number follows card-ID rules (no `:`, quotes or `\`).
  The card's status is unchanged, so a card frozen when it was reported lost stays frozen until `unfreeze`.
- **Accepted race:** an auth that resolved its number just before the replacement committed can still
  be approved on the old number. That's the same window a network has, and the auth still settles on
  the account.
- Runs started earlier replay unchanged: they never resolve, and their `CardID` is their account.

## Recurring charges (M19)

A subscription charges a card a fixed amount every cycle without the cardholder present (ADR 0010):
`go run . subscribe <id> <card> <merchant> <cents>` and `go run . unsubscribe <id>`.

- **One workflow per subscription**, `subscription:<id>`. Each run is one cycle. It charges at the start
  of the cycle (the first charge is at signup), then continues as new when the cycle ends. Billing
  stays anchored to the cycle start, even when retries took days. The default cycle is 30 days.
- **A charge is an ordinary auth.** `ChargeCard` sends an Update-with-Start `authorize`, then captures
  in full, like a network. So limits, velocity, status, the ledger, rewards, refunds, disputes and the
  audit all apply unchanged. Auth IDs are `<subscription>:c<cycle>:a<attempt>`, and the capture has a fixed
  update ID, so a retried charge never charges twice.
- **Account updater.** Each charge first looks up the card's current number (`CurrentNumber`), so a
  replaced card's subscriptions carry on with no re-signup (M18). Replaced numbers still decline for
  everyone else: nothing in the request can bypass that.
- **Declines.**
  - `OverLimit`, `Frozen` or `Velocity` puts the subscription **past-due** and retries after 1, 2 and 4 days
    (days 1, 3 and 7). A charge that succeeds clears it. One that fails after the last retry **lapses** it:
    no more charges.
  - `Closed` or `UnknownCard` **ends** it at once.
- **Cancel:** the `cancel` update stops future charges. A charge already made stands, and a refund goes
  through its auth as usual. Cancelling a subscription that has already finished fails: nothing is left to stop.
- The `subscription` query returns the cycle, status, total charged, failures in a row and the last
  decline reason.

## Decision deadline and timeout reversals (M20)

A network waits about 2 seconds for a decision. Without one, it declines the auth itself and sends a
timeout reversal. The workflow can't impose that deadline. The time is lost before its code runs (a
workflow task queued behind a busy worker) or inside a local activity, which a timer can't interrupt.
So the deadline belongs to the caller, as it does in real networks, and the issuer's job is to make
the reversal safe whenever it lands:

- **Before the hold** (it arrived with the start, or started the auth itself): `reverse` is accepted
  while deciding, and no hold is placed and no velocity attempt recorded. The auth ends `reversed`.
- **While the hold is being placed:** the reversal releases the hold as soon as it lands, and the
  `reverse` update returns once nothing is held. An update reaches a running local activity only when
  the SDK heartbeats the workflow task, so mostly this is a plain reversal just after the decision.
- **After a decline, or on an auth that has finished:** there's nothing to reverse, and it succeeds.
- `authorize` answers `Reversed` when a reversal beat the decision.

`authorizeWithin` (network.go) plays the network. It sends Update-with-Start `authorize` and waits up
to `DecisionDeadline` (2s). Without an answer, whatever the reason, it sends the reversal and reports
`Timeout`. The reversal is itself an Update-with-Start, so a start still in flight finds the auth already
reversed. It uses a fixed update ID, so a resend is a no-op. An auth ID runs once: the network's own
request, arriving late, starts nothing. `go run . authorize <auth-id> <card> <merchant> <cents>`
does the same from the command line.

No `GetVersion` is needed. The new paths start only from a `reverse` update accepted while deciding,
which runs from before M20 rejected. Rejected updates never reach history.

## Points redemption (M21)

Points become a statement credit, 1 point per cent, through the card's statement workflow: the `redeem`
update (`{ID, Points}`).

| Step | card points | points-redeemed | posted | open-to-buy |
|------|------------:|----------------:|-------:|------------:|
| Redeem P | −P | +P | −P | +P |

- **One posting**, keyed by the redemption ID, once per card ever. A resend, even in a later cycle,
  credits nothing more. A retry after the posting landed reports success, not "too few points"
  (`Posted` checks the key first).
- **A credit, not a payment.** It lowers the balance the next statement closes on, but doesn't count
  toward the minimum due, so a card that only redeems still pays a late fee.
- **Refused:** more points than the card holds (an update error), a used or empty ID, non-positive
  points, and a redemption while the statement is closing or while another is posting. Redemptions
  run one at a time, so two can't spend the same points.
- **Clawback after redeeming.** A refund that lands after a redemption claws its points back as usual,
  and can take the card below 0. The audit's points check is therefore points + redeemed ≥ 0: never
  clawed back more than was earned.
- Old statement runs replay unchanged. Registering a handler records nothing, and only a `redeem`
  update reaches the new activity.

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
- **M13**: Disputes (provisional credit, merchant response, analyst review, chargeback or rebill).
- **M14**: Operate stage: `/healthz` with dependency checks, SDK metrics via expvar, SLO contract (ADR 0006).
- **M15**: Nightly ledger auditor on a Temporal Schedule.
- **M16**: Freezing and closing cards.
- **M17**: Deploy to Fly.io on release tags (ADR 0007).
- **M18**: Card replacement: a new number on the same account (ADR 0009).
- **M19**: Recurring charges: subscriptions with retries, lapse, cancel and account updater (ADR 0010).
- **M20**: Decision deadline: timeout reversals that leave no hold, whenever they land.
- **M21**: Points redemption for statement credit.

## Success criteria

All met as of M5. Evidence in `workflow_test.go`, `ledger_test.go`, and `drill_test.go`:

- [x] Ledger sums to zero after every test and every drill.
- [x] A workflow survives a worker kill mid-hold and completes correctly on restart.
- [x] Duplicate `capture` signals post exactly once (and duplicate refund IDs).
- [x] Workflow replay test passes (determinism checked with the SDK replayer, against committed golden histories).
- [x] `go test -short ./...` stays under 10s; PR gate green.
