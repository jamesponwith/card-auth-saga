# card-auth-saga

A Temporal-orchestrated card authorization → capture/reversal saga in Go, posting
to a double-entry MySQL ledger with rewards accrual. Built inside the
[agentic flywheel](https://github.com/jamesponwith/agentic-flywheel):
Intent → Build → Validate → Release → Learn.

Start with [SPEC.md](SPEC.md). Decisions live in [docs/adr/](docs/adr/).

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
go test ./...                  # adds the ledger contract against MySQL (incl. the concurrent-hold race)
```

Drive a hold by hand (worker in one terminal, CLI in another):

```sh
go run .                       # worker on task queue card-auth
temporal workflow start --task-queue card-auth --type AuthorizeWorkflow \
  --workflow-id auth-1 --input '{"CardID":"card-1","Amount":40000,"Merchant":"shop"}'
temporal workflow signal --workflow-id auth-1 --name capture --input '{"Amount":25000}'
temporal workflow signal --workflow-id auth-1 --name refund --input '{"ID":"r1","Amount":5000}'
```

After a capture the workflow accepts `refund` signals (deduplicated by `ID`, capped at the captured
amount) for 30 days, then returns e.g. `{"Status":"captured","Captured":25000,"Refunded":5000}`.
Before capture, `--name reverse` voids the hold; with no signal it expires after 7 days
(`"HoldFor"`/`"RefundWindow"` in nanoseconds shorten either).
