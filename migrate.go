package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// migrations is the schema history, oldest first: version n is migrations[n-1].
// Append only. MySQL DDL commits as it goes, so a migration that dies halfway
// can't roll back; it is finished by running it again on the next boot. Every
// statement must therefore be safe to re-run: IF NOT EXISTS, or an unless
// guard (ADR 0005).
var migrations = [][]ddl{
	{ // 1: the ledger, as first created on boot before versioning
		{sql: `CREATE TABLE IF NOT EXISTS accounts (
			id      VARCHAR(191) PRIMARY KEY,
			balance BIGINT NOT NULL DEFAULT 0
		)`},
		{sql: `CREATE TABLE IF NOT EXISTS postings (
			idem_key   VARCHAR(191) PRIMARY KEY,
			created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
		)`},
		{sql: `CREATE TABLE IF NOT EXISTS entries (
			id       BIGINT AUTO_INCREMENT PRIMARY KEY,
			idem_key VARCHAR(191) NOT NULL,
			account  VARCHAR(191) NOT NULL,
			amount   BIGINT NOT NULL,
			FOREIGN KEY (idem_key) REFERENCES postings (idem_key)
		)`},
	},
	{ // 2: auth attempts for the velocity check
		{sql: `CREATE TABLE IF NOT EXISTS attempts (
			card    VARCHAR(191) NOT NULL,
			auth_id VARCHAR(191) NOT NULL,
			at_us   BIGINT NOT NULL, -- unix microseconds; no DATETIME/timezone parsing
			PRIMARY KEY (card, auth_id),
			INDEX (card, at_us)
		)`},
	},
	{ // 3: per-card and per-partner config
		{sql: `CREATE TABLE IF NOT EXISTS cards (
			id           VARCHAR(191) PRIMARY KEY,
			credit_limit BIGINT NOT NULL,
			CONSTRAINT cards_credit_limit_non_negative CHECK (credit_limit >= 0)
		)`},
		{sql: `CREATE TABLE IF NOT EXISTS partners (
			merchant          VARCHAR(191) PRIMARY KEY,
			points_per_dollar BIGINT NOT NULL,
			CONSTRAINT partners_rate_non_negative CHECK (points_per_dollar >= 0)
		)`},
	},
	{ // 4: each card's APR, read at every statement close
		addColumn("cards", "apr_bps", "BIGINT NOT NULL DEFAULT 2499 CHECK (apr_bps >= 0)"),
	},
}

// ddl is one migration statement. MySQL has no IF NOT EXISTS for some DDL
// (ADD COLUMN, ADD INDEX), so such a statement names an unless query instead:
// a count above zero means it already ran, and it is skipped.
type ddl struct{ sql, unless string }

func addColumn(table, column, def string) ddl {
	return ddl{
		sql: fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s", table, column, def),
		unless: fmt.Sprintf(`SELECT COUNT(*) FROM information_schema.columns
			WHERE table_schema = DATABASE() AND table_name = '%s' AND column_name = '%s'`, table, column),
	}
}

const migrateLock = "card-auth-saga:migrate"

// migrateLockWait bounds the wait for another boot's migration. A var so a
// test can shrink it.
var migrateLockWait = 60 * time.Second

// migrate brings the schema up to date. Concurrent boots serialize on a MySQL
// advisory lock, so each migration runs once; the others find nothing to do.
func migrate(ctx context.Context, db *sql.DB) (err error) {
	conn, err := db.Conn(ctx) // the lock belongs to this session
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, conn.Close()) }()
	var locked sql.NullInt64
	if err := conn.QueryRowContext(ctx, `SELECT GET_LOCK(?, ?)`, migrateLock, migrateLockWait.Seconds()).Scan(&locked); err != nil {
		return fmt.Errorf("migrate: lock: %w", err)
	}
	if locked.Int64 != 1 { // 0: timed out; NULL: error
		return fmt.Errorf("migrate: another boot held the migration lock for %v", migrateLockWait)
	}
	// Released explicitly: Close returns the session to the pool, lock and all.
	defer func() {
		_, rerr := conn.ExecContext(context.WithoutCancel(ctx), `DO RELEASE_LOCK(?)`, migrateLock)
		err = errors.Join(err, rerr)
	}()

	if _, err := conn.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (
		version    INT PRIMARY KEY,
		applied_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
	)`); err != nil {
		return fmt.Errorf("migrate: %w", err)
	}
	var current int
	if err := conn.QueryRowContext(ctx, `SELECT COALESCE(MAX(version), 0) FROM schema_migrations`).Scan(&current); err != nil {
		return fmt.Errorf("migrate: %w", err)
	}
	if current > len(migrations) {
		return fmt.Errorf("migrate: database is at version %d, newer than this binary's %d; run the newer binary", current, len(migrations))
	}
	for v := current + 1; v <= len(migrations); v++ {
		for _, d := range migrations[v-1] {
			if d.unless != "" {
				var done int
				if err := conn.QueryRowContext(ctx, d.unless).Scan(&done); err != nil {
					return fmt.Errorf("migrate: version %d: %w", v, err)
				}
				if done > 0 {
					continue
				}
			}
			if _, err := conn.ExecContext(ctx, d.sql); err != nil {
				return fmt.Errorf("migrate: version %d: %w", v, err)
			}
		}
		if _, err := conn.ExecContext(ctx, `INSERT INTO schema_migrations (version) VALUES (?)`, v); err != nil {
			return fmt.Errorf("migrate: record version %d: %w", v, err)
		}
	}
	return nil
}
