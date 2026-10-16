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

- **One workflow per authorization** (`AuthorizeWorkflow`). An `authorize` update, sent with
  Update-with-Start, returns approve/decline as soon as the hold is placed. The hold then takes
  increments and partial captures until a final capture, a reversal, or a 7-day timer closes it.
  After a capture it takes refunds for 30 days. Each of these is a validated update, or a signal
  for fire-and-forget callers. The timers are durable: a worker can die mid-wait and the workflow
  continues on another one.
- **One workflow per card for statements** (`StatementWorkflow`). Each run is one cycle: late fee at
  the due date, interest and a new statement at close. Then it continues as new, so history never
  grows past a cycle.
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

**Check-then-act, again, inside the workflow.** With signals, one loop handled one request at a time.
Updates run as concurrent handlers. Workflow code is single-threaded, but every activity is a yield
point, so a second capture could pass its validator while the first was still posting. Each step
now claims its share before its first activity: a capture or release marks the hold busy, and a
refund reserves its amount and ID. Updates also exposed two timing gaps that a single signal loop
could never hit: a refund arriving before settlement rewards had posted, and the refund window
closing mid-refund. The test environment can't produce either, since activities there take no
time, so a drill with a slow ledger forces both.

## Failure drills

These run the real worker against a real Temporal dev server (the SDK downloads and starts it), on the
in-memory ledger or on MySQL. Each drill checks exact balances afterwards, then replays the recorded
history:

| Drill | What happens | Result |
|---|---|---|
| Worker killed mid-hold | Stop the only worker, signal capture while nothing is running, start a fresh worker | Capture lands once; hold cleared; rewards accrued |
| Duplicate signals | 3× the same capture, 3× the same refund ID | One capture, one refund, points 250 |
| Activity times out after writing | The ledger commits each write, then stalls past a 1s activity timeout, so Temporal retries | Every step posts exactly once |
| Update-with-Start, sent twice | The caller starts the auth and waits on the `authorize` update; a retry reuses the auth ID | `true` both times, one hold |
| Update-with-Start, declined | An over-limit auth | `false`, then the workflow closes as declined |
| Increments and split capture | +$200 increment approved, +$500 declined by the limit, $300 partial capture, $250 final capture | $550 captured, the last $50 released, 550 points |
| Statement cycles | 1s cycles of ten 100ms days: pay $10 in the first (sent twice, one update ID), then pay nothing | One payment; $25 fee on day 4; $3.47 interest on the daily balances; next statement $518.47; the closed run replays |
| Disputes | Seconds-long deadlines: one dispute contested and won, one the merchant never answers, one contested and lost | $250 charged back, $50 rebilled, 150 points kept; the history replays |
| Worker process | `run` as `main` calls it: ops port taken, then started, probed, cancelled | Refuses to start without its ops port; `/healthz` ok with the version; SDK metrics served; stops on cancel and closes the port |
| Audit | A live hold and a failing workflow; the audit command; the schedule created twice and triggered, then triggered again with no worker | Both named; the command exits with the failure; one schedule; `/healthz` degraded by its findings, and still degraded while the next audit runs |
| Card lifecycle (MySQL) | `set-limit` a new card, then raise its limit | One running statement cycle, same run both times |
| Updates mid-step | Every write stalls 1.5s; a refund update is sent the moment its capture returns, twice with one update ID, and the 1s window closes while it posts | One refund; points clawed back; a new update ID for the same refund is rejected |

M8 changed which activities a capture runs, so it is the first change those recordings had to
survive. `workflow.GetVersion` tags new runs. Runs without the tag stay on the old `CaptureHold`
and `ReleaseHold` and reject partial captures and increments. Removing the gate makes 5 of the 6
older recordings fail to replay.

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
| Capture/release claims the hold before posting | *concurrent captures post once*, *capture during reverse rejected* |
| Refunds reserve their amount before posting | *concurrent refunds can't over-refund* |
| Workflow waits for in-flight refunds | mid-step drill: the refund posts, its points are never clawed back |
| Capture returns only after settlement rewards | mid-step drill: the refund is rejected as too early |
| Old runs keep the single-capture activities (`GetVersion`) | 5 of the 6 pre-M8 golden histories fail to replay |
| A partial capture leaves the rest of the hold open | *split shipment*, *partials can't exceed the hold* |
| Release takes only what's still held | *expiry/reverse after a partial releases the rest* |
| Payments still posting finish before the statement closes | *in-flight payment*: interest charged on $500, not $490 |
| A statement's paid total comes from the ledger | `TestStatementPaymentResentNextCycle`: a payment resent in the next cycle waives its late fee |
| Old statement runs keep counting Paid themselves (`GetVersion`) | the pre-change statement recording fails to replay (`TMPRL1100`) |
| Payments arriving mid-close are rejected | *mid-close payment* accepted while interest posts |
| Velocity: more than 5 attempts in 60s declines | `TestVelocityCheck`: the 6th attempt is approved |
| A retried attempt counts from its first recorded time | `TestRecordAttempt`: a retry sees the attempts that arrived since |
| Migrations run under an advisory lock | 8 concurrent boots: duplicate version rows without it |
| A boot that can't get the lock refuses to migrate | *migrate waits for the lock*: tables created unguarded |
| The lock is released, not left on a pooled session | *a running worker doesn't block the next boot* |
| Every migration is re-runnable | re-applying all migrations on a migrated database; adopting pre-versioning schemas |
| A database newer than the binary is refused | *refuses newer database* |
| `CHECK` constraints on limits and rates | negative limit or rate accepted |
| A config read error fails the auth, never declines it | *config outage is not a decline*: every card declined as unknown |
| A failed rate read is retried, not defaulted | *reward rate outage retries*: settlement fixes the default rate for good |
| Statement ledger steps retry until the ledger answers | *rides out ledger outage*: 8 failures end the card's billing |
| The late fee posts before that day's sample | *nothing paid: fee and interest* |
| Interest waived when the last statement was paid in full | *paid in full*, *overpaid* |
| Credit days count as zero | *credit days count as zero*: a refund's credit offsets interest owed |
| APR read from the card's config | *statement APR from config* |
| Refunds and disputes share one cap | *dispute over what refunds left*, *refund over what disputes left* |
| The auth stays open until its disputes resolve | *contested, no ruling*: the auth ends with the dispute unsettled |
| Missed deadlines go to the cardholder | *no merchant response*, *contested, no ruling* |
| A lost dispute is rebilled | *contested, merchant wins* |
| A won dispute claims its place in the points chain when it's won | *two chargebacks won together*, *chargeback then refund*: a point clawed twice |
| Disputes open only once the charge settles | *dispute waits for settlement*: a quick win would claw back at rate 0 |
| The dispute update returns after its credit posts | *dispute returns after credit* |
| A credit the ledger refuses fails the update | *dispute credit failure is reported* |
| Accepting ends a dispute at once, not at the review deadline | *disputes query*: an accepted dispute still open |
| `/healthz` checks Temporal, then MySQL | *worker health, Temporal down*; operate drill: a MySQL outage reads "ok" |
| The worker listens before starting work | worker drill: starts without its ops port, invisible to the prober |
| The SDK reports into the served metrics | worker drill: `/debug/vars` has no Temporal series |
| Metrics series: sorted tags, gauges set, timers totalled | `TestExpvarMetrics` |
| Each audit invariant (card money, holds, points, memos, points net, total) | `TestAuditLedger`, one row per break |
| Stuck auths and failed workflows are found | audit drill: either one missed |
| Health follows the latest finished audit | audit drill: health ok while an audit has findings, or while the next one runs |
| Frozen and closed cards decline new auths | *card status*: frozen and closed both approved |
| Holds placed before a freeze still settle, but can't grow | *frozen after the hold*: an increment granted |
| A closed card's statements end only once nothing is posted or held | *statement ends for closed card*: ends with a balance, a hold or a credit |
| Closed is final, and a closed card's limit can't change | *card status transitions*: a closed card reopened or re-limited |
| An increment claims the hold | *capture during increment rejected*: otherwise a final capture strands the increment |

Two experiments removed code instead. Cancelling the hold timer after capture survived mutation,
because a finished workflow cleans up its own timers, so it was deleted. Likewise, waiting for all
update handlers to finish before a decline closes the workflow: without it, the update still answered
in the test environment and on a real server, because a waiting handler runs in the same workflow
task that sets the decision. That wait was deleted too.

One mutation survives on purpose. The merchant-response handler re-checks the dispute's stage
after its validator. On a real server, a deadline timer and the response can arrive in the same
workflow task, and the dispute's goroutine can run between the validator and the handler; without
the re-check, the update would report success for a response that was ignored. The test environment
always runs the handler first, so it can't produce that order.

A note on the test environment: it caps "retry forever" at 10 attempts so a unit test can't hang,
so the outage test fails 8 times. That's past the saga's limit of 5, and under the test cap.

The auditor's first real run checked the 778 accounts that every test and drill had written to the
shared MySQL database during development, and found nothing.

## How it was built

Each milestone in SPEC.md went through the agentic flywheel: a tracked issue, a branch, a fast pre-commit
gate (fmt, vet, `go test -short` in about a second), a local AI review before every push, and a CI gate
(lint, plus `-race` tests against a MySQL service container) before a human merge.
