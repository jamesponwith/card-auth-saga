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

Drive a hold by hand (worker in one terminal, CLI in another):

```sh
go run .                       # worker on task queue card-auth
temporal workflow start --task-queue card-auth --type AuthorizeWorkflow \
  --workflow-id auth-1 --input '{"CardID":"card-1","Amount":40000,"Merchant":"shop"}'
temporal workflow signal --workflow-id auth-1 --name capture --input '{"Amount":25000}'
temporal workflow result --workflow-id auth-1   # {"Status":"captured","Captured":25000}
```

Skip the signal and the hold expires after 7 days (set `"HoldFor"` in nanoseconds to shorten it).
