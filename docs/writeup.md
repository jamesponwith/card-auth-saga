# A card authorization saga on Temporal, and proving it keeps the money straight

Card authorization looks like one request, but it's a long-running process with money at every step. The
issuer places a hold, waits up to a week for the merchant to capture, settles the captured amount, releases
the rest, accrues rewards, and keeps taking refunds for another month. Every step has to survive retries,
crashes, and duplicate messages without posting money twice or losing it.

At Capital One I built resiliency machinery for card authorizations the hard way: distributed leases on
DynamoDB, and retries and failover logic that we wrote and operated ourselves. This project asks a narrower
question: how much of that does a durable-execution engine take off your hands, and what still has to be
designed carefully? The answer turned out to be "the orchestration, but not the ledger".

## Shape

- **One workflow per authorization** (`AuthorizeWorkflow`). It places a hold, then waits on a
  selector: a `capture` signal, a `reverse` signal, or a 7-day timer. After a capture it takes `refund`
  signals for 30 days. The timers are durable: a worker can die mid-wait and the workflow continues on
  another one.
- **A double-entry ledger** (MySQL, `database/sql`). Every posting sums to zero, and every posting is
  keyed `<workflow ID>:<step>` with a unique index, so a retried activity hits the duplicate key and
  becomes a no-op.
- **Compensations, not mutations.** Reversal, a capture that fails, and refunds each post new
  balancing entries. History is never edited.
- **Rewards in points**, funded by the brand partner's account, so points balance the same way money does.

## What Temporal did not solve

**Check-then-act on the limit.** M1 checked the card's limit in one activity and placed the hold in the next.
Temporal ran both reliably, which didn't help: two concurrent authorizations on one card could both pass the
check. The fix belongs in the ledger. `PostWithin` locks the card's account rows (`SELECT … FOR UPDATE`, in
sorted order) so the check and the hold commit in one transaction. Removing the lock as an experiment
let MySQL accept **20 holds worth 200 against a limit of 100**, in 3 of 3 runs. With it, exactly 10 get through.

**A deadlock hiding in "create if missing".** `INSERT IGNORE` on an account row that already exists takes a
shared lock, and `FOR UPDATE` then needs to upgrade it. Two holds on one card would each hold the shared lock
and wait on the other. Account rows are now created before the transaction starts.

**Rounding.** Clawing back each refund's own rounded points leaves stray points after a full refund
($2.00 refunded as $1.50 + $0.50 claws back 1 + 0 of 2 points). The clawback is instead computed
as points(remaining before) − points(remaining after), using the rate fixed at settlement, so a full refund
always lands on exactly zero, even if the partner changes its rate in between.

## Failure drills

These run the real worker against a real Temporal dev server (the SDK downloads and starts it), on the
in-memory ledger or on MySQL. Each drill checks exact balances afterwards, then replays the recorded
history:

| Drill | What happens | Result |
|---|---|---|
| Worker killed mid-hold | Stop the only worker, signal capture while nothing is running, start a fresh worker | Capture lands once; hold cleared; rewards accrued |
| Duplicate signals | 3× the same capture, 3× the same refund ID | One capture, one refund, points 250 |
| Activity times out after writing | The ledger commits each write, then stalls past a 1s activity timeout, so Temporal retries | Every step posts exactly once |

The recorded histories are committed under `testdata/`. On every commit, `TestReplayGoldenHistories`
replays them against the current workflow code. As a check, I added one extra activity call at the start
of the workflow: all three replays failed with Temporal's nondeterminism error (`TMPRL1100`).

## Every guard is mutation-tested

Each safeguard was removed on purpose to confirm a test notices:

| Removed | Caught by |
|---|---|
| Idempotency on posting keys | retry tests; the timeout drill posts the capture twice (60,000 vs 30,000) |
| Row locks in `PostWithin` | concurrent-hold contract test against MySQL |
| Release when a capture fails | `TestFailedCaptureReleasesHold` |
| Refund-ID dedupe | *resent refund ID posts once* |
| Per-request hold duration | *hold expires before capture* |
| Remaining-spend clawback | *odd refunds claw back every point* |
| Rate fixed at settlement | `TestRewardRateFixedAtSettlement` |
| Workflow determinism | golden history replay |

Two of these experiments also removed code. Cancelling the hold timer after capture survived mutation,
because a finished workflow cleans up its own timers, so it was deleted.

## How it was built

Each milestone in SPEC.md went through the agentic flywheel: a tracked issue, a branch, a fast pre-commit
gate (fmt, vet, `go test -short` in about a second), a local AI review before every push, and a CI gate
(lint, plus `-race` tests against a MySQL service container) before a human merge.
