package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/go-sql-driver/mysql"
)

const mysqlDupKey = 1062

var schema = []string{
	`CREATE TABLE IF NOT EXISTS accounts (
		id      VARCHAR(191) PRIMARY KEY,
		balance BIGINT NOT NULL DEFAULT 0
	)`,
	`CREATE TABLE IF NOT EXISTS postings (
		idem_key   VARCHAR(191) PRIMARY KEY,
		created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
	)`,
	`CREATE TABLE IF NOT EXISTS entries (
		id       BIGINT AUTO_INCREMENT PRIMARY KEY,
		idem_key VARCHAR(191) NOT NULL,
		account  VARCHAR(191) NOT NULL,
		amount   BIGINT NOT NULL,
		FOREIGN KEY (idem_key) REFERENCES postings (idem_key),
		INDEX (account)
	)`,
}

// MySQLLedger keeps balances in `accounts` and the append-only history in
// `postings` + `entries`. Each posting is one transaction (ADR 0004).
type MySQLLedger struct {
	db *sql.DB
}

// OpenMySQL connects and creates the schema if it's missing.
func OpenMySQL(ctx context.Context, dsn string) (*MySQLLedger, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, err
	}
	// ponytail: CREATE IF NOT EXISTS on boot; switch to versioned migrations at the first ALTER.
	for _, stmt := range schema {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			db.Close()
			return nil, fmt.Errorf("migrate: %w", err)
		}
	}
	return &MySQLLedger{db: db}, nil
}

func (l *MySQLLedger) Close() error { return l.db.Close() }

func (l *MySQLLedger) Post(ctx context.Context, key string, entries ...Entry) error {
	_, err := l.PostWithin(ctx, key, nil, 0, entries...)
	return err
}

func (l *MySQLLedger) PostWithin(ctx context.Context, key string, capped []string, limit int64, entries ...Entry) (bool, error) {
	if !balanced(entries) {
		return false, ErrUnbalanced
	}
	ids := accountIDs(capped, entries)
	marks := strings.TrimSuffix(strings.Repeat("?,", len(ids)), ",")
	args := make([]any, len(ids))
	values := make([]string, len(ids))
	for i, id := range ids {
		args[i] = id
		values[i] = "(?)"
	}
	// Create missing accounts outside the transaction: INSERT IGNORE on an
	// existing row takes a shared lock, and upgrading it to FOR UPDATE inside
	// the transaction would deadlock two concurrent holds on one card.
	if _, err := l.db.ExecContext(ctx, `INSERT IGNORE INTO accounts (id) VALUES `+strings.Join(values, ","), args...); err != nil {
		return false, err
	}

	tx, err := l.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback() // no-op after Commit

	// Claim the key first. A concurrent poster of the same key blocks here
	// until we commit, then gets a duplicate key and reports success.
	if _, err := tx.ExecContext(ctx, `INSERT INTO postings (idem_key) VALUES (?)`, key); err != nil {
		var me *mysql.MySQLError
		if errors.As(err, &me) && me.Number == mysqlDupKey {
			return true, nil
		}
		return false, err
	}

	rows, err := tx.QueryContext(ctx, `SELECT id, balance FROM accounts WHERE id IN (`+marks+`) ORDER BY id FOR UPDATE`, args...)
	if err != nil {
		return false, err
	}
	balances := map[string]int64{}
	for rows.Next() {
		var id string
		var b int64
		if err := rows.Scan(&id, &b); err != nil {
			rows.Close()
			return false, err
		}
		balances[id] = b
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return false, err
	}

	if !within(balances, capped, limit, entries) {
		return false, nil // rollback releases the key, so a later retry re-checks
	}
	for _, e := range entries {
		if _, err := tx.ExecContext(ctx, `INSERT INTO entries (idem_key, account, amount) VALUES (?, ?, ?)`, key, e.Account, e.Amount); err != nil {
			return false, err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE accounts SET balance = balance + ? WHERE id = ?`, e.Amount, e.Account); err != nil {
			return false, err
		}
	}
	return true, tx.Commit()
}

func (l *MySQLLedger) Balance(ctx context.Context, account string) (int64, error) {
	var b int64
	err := l.db.QueryRowContext(ctx, `SELECT balance FROM accounts WHERE id = ?`, account).Scan(&b)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	return b, err
}
