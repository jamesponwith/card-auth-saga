# 0004. MySQL ledger via database/sql + go-sql-driver/mysql

Date: 2026-10-06
Status: accepted

## Context

The in-memory ledger loses money on restart, and checking the card's limit and placing the hold
were two separate activities, so concurrent auths on one card could both pass. Imprint runs MySQL.

## Decision

Implement `Ledger` on MySQL with `database/sql` + `github.com/go-sql-driver/mysql`. No ORM. A posting
is one transaction: a unique `postings.idem_key` row makes it idempotent, and `PostWithin` locks the
capped account rows (`SELECT … FOR UPDATE`, sorted ids) so the limit check and the hold commit together.

## Consequences

Holds can no longer oversubscribe a limit, and a retried activity hits the duplicate key, so it can't
double-post. Integration tests need a real MySQL (`CAS_MYSQL_DSN`). They're skipped under `-short`, so the
pre-commit hook stays fast, and CI runs them against a `mysql:8.4` service container.
