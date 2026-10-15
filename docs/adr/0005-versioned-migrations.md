# 0005. Versioned schema migrations, stdlib only

Date: 2026-10-14
Status: accepted

## Context

The schema was `CREATE TABLE IF NOT EXISTS` on every boot, which can add tables but never change one.
Per-card config tables are the first change that needs real versioning. Two workers can boot at once, and
MySQL DDL commits implicitly, so a migration can't be rolled back if it dies halfway.

## Decision

An ordered list of migrations in `migrate.go`, recorded in `schema_migrations(version)`. `OpenMySQL`
applies the missing ones while holding `GET_LOCK('card-auth-saga:migrate')`. Every statement must be
re-runnable (`IF NOT EXISTS`, or an "already exists" error such as a duplicate column treated as
done), so a migration that died halfway completes on the next boot.
A database newer than the binary is refused. No migration library: about 60 lines of `database/sql`.

## Consequences

Schema changes are reviewable, append-only Go entries, and concurrent boots migrate exactly once.
Migration 1 is the original schema, so databases created before this adopt it in place. Each migration
is written to be re-runnable, and a test re-applies every migration on an already migrated database.
There are no down-migrations: roll forward instead.
