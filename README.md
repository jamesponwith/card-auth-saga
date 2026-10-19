# card-auth-saga

A card issuer's backend on Temporal, in Go: authorizations with instant decisions, captures,
refunds and disputes; monthly statements with interest; rewards; fraud velocity checks; and a
double-entry MySQL ledger audited nightly. Built inside the
[agentic flywheel](https://github.com/jamesponwith/agentic-flywheel):
Intent → Build → Validate → Release → Operate → Learn.

![One card's story, printed from the real ledger postings. A $400 hold approved instantly, raised
$100 by the hotel, and captured in two shipments. The last capture survives a crash after its write
without posting twice. Rewards accrue, a $50 refund and a $100 chargeback claw them back, a sixth
rapid auth on another card is declined for velocity, and the audit finds the ledger balanced](docs/demo.gif)

The demo is real: `vhs docs/demo.tape` re-records it from `TestDemo`, which prints each posting as
the workflow makes it.

Start with [SPEC.md](SPEC.md) for the design and [docs/writeup.md](docs/writeup.md) for what it
took to keep the money straight. Decisions live in [docs/adr/](docs/adr/).

## Run

```sh
brew install temporal          # one-time: Temporal CLI
temporal server start-dev      # local server + web UI on http://localhost:8233
go test -short ./...           # unit + workflow tests (no server needed)
```

MySQL ledger (ADR 0004) — integration tests and a durable worker:

```sh
docker run -d --name cas-mysql -e MYSQL_ROOT_PASSWORD=cas -e MYSQL_DATABASE=cas -p 3307:3306 mysql:8.4
export CAS_MYSQL_DSN='root:cas@tcp(127.0.0.1:3307)/cas'
go test ./...                  # adds the ledger contract, migrations and config against MySQL
```

`go test ./...` takes about a minute: with MySQL it also runs the drills (below), which download and
start a Temporal dev server of their own.

No Docker? Run MySQL 8.4 from Homebrew in a directory of its own:

```sh
brew install mysql@8.4
mysqld=$(brew --prefix mysql@8.4)/bin/mysqld
mkdir -p ~/cas-mysql && cd ~/cas-mysql
$mysqld --initialize-insecure --datadir=./data
$mysqld --datadir=./data --port=3307 --bind-address=127.0.0.1 --socket=m.sock --mysqlx=OFF &
$(brew --prefix mysql@8.4)/bin/mysql -h127.0.0.1 -P3307 -uroot \
  -e "ALTER USER root@localhost IDENTIFIED BY 'cas'; CREATE DATABASE cas"
```

With MySQL, card limits and partner reward rates live in the `cards` and `partners` tables (SPEC.md,
M11). The schema migrates itself on boot (ADR 0005). Before the first auth, set them up:

```sh
go run . set-limit card-1 100000   # $1,000.00
go run . set-rate shell 3          # 3 points per $1 at shell
# each prints ok: <the command> once saved
```

Without MySQL, the worker uses a fixed demo config: `card-1` has a $1,000 limit, `shell` earns 3
points per $1 and `booking` earns 5.

Drive a hold by hand (worker in one terminal, CLI in another):

```sh
go run .                       # worker on task queue card-auth
temporal workflow execute-update-with-start --update-name authorize \
  --task-queue card-auth --type AuthorizeWorkflow --id-conflict-policy UseExisting \
  --workflow-id auth-1 --input '{"CardID":"card-1","Amount":40000,"Merchant":"shell"}'
temporal workflow update execute --workflow-id auth-1 --name increment --input '{"ID":"i1","Amount":10000}'
temporal workflow update execute --workflow-id auth-1 --name capture --input '{"Amount":20000,"Partial":true}'
temporal workflow update execute --workflow-id auth-1 --name capture --input '{"Amount":25000}'
temporal workflow update execute --workflow-id auth-1 --name refund --update-id r1 \
  --input '{"ID":"r1","Amount":5000}'
```

The first command answers `{"Approved":true}` as soon as the hold is decided, or `{"Approved":false,"Reason":…}`
with `OverLimit`, `Velocity` or `UnknownCard` (or `Frozen`, `Closed` or `Replaced`, below). The workflow keeps running. Running it again on the same `--workflow-id`, as a network retry would, returns
the same answer without placing a second hold (SPEC.md, M6).

A real network waits about 2 seconds, then gives up and sends a timeout reversal (SPEC.md, M20).
`go run . authorize` plays that network:

```sh
go run . authorize auth-9 card-1 shell 1000   # approved, declined (Reason), or Timeout and reversed
```

A reversal is safe whenever it lands. Before the hold, none is placed. While the hold is being placed,
it's released once it lands. After a decline, there's nothing to undo.

`capture`, `reverse` and `refund` are updates: each returns once the ledger posts. A bad request is
rejected with the reason: a capture outside the hold, a second capture, a refund over what's left, or
a used refund ID. A retry with the same `--update-id` gets the first result back. The same names also
work as fire-and-forget signals (`temporal workflow signal`), which log and drop bad requests (M7).

`increment` raises the hold, here to $500, and returns `false` if the card's limit can't cover it. A
`Partial` capture posts without closing the hold, as for a split shipment. The final capture, here
$250 for $450 captured in total, releases what's left. A reversal or expiry after partial captures
releases the rest and still settles what was captured (M8).

After a capture the workflow accepts refunds (deduplicated by `ID`, capped at the captured
amount) for 30 days, then returns the totals. After this walkthrough and the dispute below:
`{"Status":"captured","Captured":45000,"Refunded":5000,"ChargedBack":5000,"Points":1050,"Reason":""}`.
Points accrue on the captured total at the merchant's partner rate at settlement and are clawed back on refund (SPEC.md).
Before capture, `--name reverse` voids the hold; with no signal it expires after 7 days
(`"HoldFor"`/`"RefundWindow"` in nanoseconds shorten either).

Dispute a captured charge while refunds are open (SPEC.md, M13). The credit lands at once, the
merchant has 45 days to accept or contest, and a contested dispute goes to an analyst for 30 days. A
missed deadline goes to the cardholder:

```sh
temporal workflow update execute --workflow-id auth-1 --name dispute --input '{"ID":"d1","Amount":5000}'
temporal workflow update execute --workflow-id auth-1 --name merchant-response --input '{"DisputeID":"d1","Accept":false}'
temporal workflow update execute --workflow-id auth-1 --name resolve-dispute --input '{"DisputeID":"d1","CardholderWins":true}'
temporal workflow query --workflow-id auth-1 --name disputes
```

If the cardholder wins, the credit stands and the dispute's points are clawed back. If the merchant
wins, the charge is rebilled. Refunds and disputes share one cap, so together they never return more
than was captured.

A card that attempts more than 5 auths in 60 seconds is declined regardless of its limit. This
catches card testing, and it covers declined attempts too (SPEC.md, M10).

Each card has a monthly statement cycle (SPEC.md, M9 and M12). With MySQL, `set-limit` starts it
when it creates the card. To run one by hand, `Cycle`/`Grace`/`Day` are in nanoseconds; leave them out
for 30 days, 25 days and 1 day:

```sh
go run . set-limit card-1 100000   # creates the card and starts statement:card-1
go run . set-apr card-1 1999       # 19.99% APR from the next statement (default 24.99%)
go run . freeze card-1             # new auths decline (Reason Frozen); holds already placed still settle
go run . unfreeze card-1
go run . close card-1              # final: declines (Closed); statements end once nothing is owed or held
temporal workflow update execute --workflow-id statement:card-1 --name pay --input '{"ID":"p1","Amount":2500}'
temporal workflow query --workflow-id statement:card-1 --name statement
temporal workflow update execute --workflow-id statement:card-1 --name redeem --input '{"ID":"r1","Points":500}'
```

`redeem` turns points into a statement credit at 1 point per cent (SPEC.md, M21). It's a credit, not
a payment, so it doesn't count toward the minimum due.

Each cycle samples the card's balance at the end of every day. It charges a $25 late fee if the
minimum isn't paid by the due date, and at close charges interest on the average daily balance
unless the last statement was paid in full. The workflow then continues as new, so its history covers
one cycle. `temporal workflow list --query 'WorkflowId="statement:card-1"'` lists each run.

A lost or stolen card gets a new number on the same account (SPEC.md, M18). The limit, balance,
statement cycle and points all carry over:

```sh
go run . replace card-1 card-1b    # card-1 now declines (Reason Replaced); card-1b authorizes
temporal workflow execute-update-with-start --update-name authorize \
  --task-queue card-auth --type AuthorizeWorkflow --id-conflict-policy UseExisting \
  --workflow-id auth-2 --input '{"CardID":"card-1b","Amount":1000,"Merchant":"shell"}'
```

Auths already placed on the old number still capture, refund and dispute as usual. The account keeps
the ID it was created with, here `card-1`: `statement:card-1`, the ledger's `card:card-1:*` accounts,
and the `CardID` every auth is tagged with. Admin commands accept any of a card's numbers.

A subscription charges a card every 30 days without the cardholder there (SPEC.md, M19). Each
charge is an ordinary auth and capture, so limits, velocity, rewards and refunds all apply:

```sh
go run . subscribe streaming-1 card-1b shell 1599   # $15.99 now, then every 30 days
temporal workflow query --workflow-id subscription:streaming-1 --name subscription
go run . unsubscribe streaming-1
```

A declined charge makes the subscription `past-due` and is retried on days 1, 3 and 7. If the last
retry declines too, the subscription is `lapsed`. A closed card ends it (`ended`). Each charge goes to the
card's current number, so subscriptions survive `replace` with no re-signup.

The cardholder can see a card's subscriptions and stop one by blocking its merchant (SPEC.md, M22):

```sh
go run . subscriptions card-1b     # id, merchant, amount, status; then any blocked merchants
go run . block card-1b shell       # shell's auths decline (MerchantBlocked); its subscriptions end at their next charge
go run . unblock card-1b shell
```

## Deploy

Pushing a `v*` tag releases and deploys (ADR 0007). `release.yml` runs goreleaser, then
`deploy/fly/deploy.sh` brings up three Fly apps on the private network:

- **MySQL 8.4**, on a volume.
- **Temporal**: the pinned, checksummed CLI server, persisted to SQLite on a volume.
- **The worker**, whose `/healthz` is public.

The deploy fails unless `https://<prefix>-worker.fly.dev/healthz` reports `ok` at the tag's version.
If it doesn't, the script redeploys the image that was running (by digest) and still fails, reporting
"rolled back to <version>" (ADR 0007).

One-time setup: a Fly.io account with a payment method, the three apps, then three repo secrets. The
release job deploys with tokens scoped to those three apps, which can't create apps or touch any other.
The monthly restore drill keeps an org token, because it creates and destroys scratch apps.

```sh
for a in mysql temporal worker; do fly apps create cas-jamesponwith-$a --org personal; done
tok=$(for a in mysql temporal worker; do fly tokens create deploy -a cas-jamesponwith-$a -x 8760h; done |
  paste -sd, - | sed 's/,FlyV1 /,/g')       # deploy tokens joined: each app, nothing else
printf %s "$tok" | gh secret set FLY_DEPLOY_TOKEN; unset tok
fly tokens create org -x 8760h | gh secret set FLY_API_TOKEN      # restore drill only
openssl rand -base64 24 | gh secret set CAS_MYSQL_PASSWORD
git tag v0.2.0 && git push origin v0.2.0      # release and deploy
```

App names default to `cas-jamesponwith-{mysql,temporal,worker}` and the region to `iad`. Override them
with the `FLY_APP_PREFIX` and `FLY_REGION` repo variables, and update `docs/slo.yml`'s url to match.
`go test -run 'TestDeployScript|TestRestoreDrill'` checks both scripts' logic against a stub `flyctl`.

Both volumes take daily snapshots, kept 14 days. Once a month, `restore-drill.yml` runs
`deploy/fly/restore-drill.sh`. It restores the latest snapshot of each volume into throwaway apps on
production's exact images, checks that the ledger balances, that the schema matches production and
that Temporal still holds its workflows, then destroys the throwaway apps. Run it any time with
`gh workflow run restore-drill`.

## Operate

The worker serves the flywheel's Operate contract on `CAS_HTTP_ADDR` (default `:8080`, ADR 0006):

```sh
curl localhost:8080/healthz      # {"status":"ok","version":"v1.2.3","uptime_s":42}, or "degraded: temporal: …"
curl localhost:8080/debug/vars   # the Temporal SDK's metrics under "temporal", one series per name and tags
```

Set `CAS_DEBUG_ADDR` to give `/debug/vars` a port of its own. The Fly deployment does this
(`:9091`, private network only), so only `/healthz` is public.

`/healthz` reports `ok` only if Temporal and MySQL both answer. Release builds stamp the version.
`docs/slo.yml` points at the Fly worker, and `operate.yml` probes it every 30 minutes, opening an
`incident` issue on a sustained breach and closing it on recovery. Run it by hand with
`gh workflow run operate`. If the deployment goes away, take the schedule off first: probing an
address nothing serves files incidents that never close.

The scheduled jobs (`security`, `restore-drill`, `learn` and the `operate` prober itself) report their
own failures. `job-alert.yml` opens a `job-failure` issue when one fails on `main`, comments on repeat
failures, and closes the issue on the next green run. Test it with
`gh workflow run job-alert -f job=security -f conclusion=failure`, then `-f conclusion=success`.

## Audit

Every 24 hours a Temporal Schedule (`ledger-audit`, created by the worker on boot) runs `AuditWorkflow`
(SPEC.md, M15). It checks each card's money nets to zero, that holds are never negative and points
never clawed back past what was earned,
that payment memos and points net to zero, and that the whole ledger sums to zero. It also checks that
no auth or statement run is stuck, that no workflow failed since the last audit, and that no card
holds money with no auth running on it. Findings are the
workflow's result, and `/healthz` reports `degraded: audit: …` while the latest one has any:

```sh
go run . audit                     # run it now: prints "audit: clean", or the findings and exits 1
temporal schedule describe --schedule-id ledger-audit
```

## Security

Every PR runs `gitleaks` over the whole history and `govulncheck` over the code. Both block the merge,
and `govulncheck` only reports vulnerabilities the code can reach. `security.yml` rescans `main` weekly,
since new CVEs arrive without any code changing. `go.mod`'s `toolchain` line and the Dockerfile pin
the same patched Go, so CI scans exactly what production runs.

## Load test

```sh
CAS_LOAD=1 go test -run TestLoad -v .   # needs CAS_MYSQL_DSN; numbers and invariants in docs/writeup.md
```

It measures auths per second and decision latency, spread across cards and concentrated on one, and
checks that exactly the right number are approved and that the audit is clean afterwards.

To measure the Fly stack instead of a laptop, without putting load cards in production's ledger
(ADR 0008):

```sh
FLY_API_TOKEN=$(fly auth token) deploy/fly/load-drill.sh   # ~10 min; a few cents
```

It deploys a throwaway `-drill-load-` copy of the stack, runs the same test against it through
`flyctl proxy`, and destroys the copy, pass or fail.

## Failure drills

```sh
go test -run TestDrills -v .   # downloads + starts a Temporal dev server; add CAS_MYSQL_DSN for MySQL
```

Thirteen drills against a real server: worker killed mid-hold, duplicate capture/refund signals,
activities that commit then time out, Update-with-Start approved (sent twice) and declined,
updates that land mid-step (a refund during settlement, the window closing mid-refund),
increments with a split capture, three disputes on one charge (won, unanswered, lost), the worker process end to end (ops port,
`/healthz`, metrics, shutdown), a MySQL outage named by `/healthz`, the audit (findings, command, schedule, health), two statement cycles across ContinueAsNew, and (on MySQL)
a card created twice with `set-limit` getting a single statement cycle. Each asserts exact balances, then replays the recorded history.
`CAS_UPDATE_GOLDEN=1` re-records `testdata/history-*.json`, which `go test -short` replays on every
commit to catch nondeterministic workflow changes.
