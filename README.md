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
