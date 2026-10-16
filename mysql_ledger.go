package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
)

const mysqlDupKey = 1062

// MySQLLedger keeps balances in `accounts` and the append-only history in
// `postings` + `entries`. Each posting is one transaction (ADR 0004). It also
// serves Config from `cards` and `partners`.
type MySQLLedger struct {
	db *sql.DB
}

// OpenMySQL connects and brings the schema up to date (ADR 0005).
func OpenMySQL(ctx context.Context, dsn string) (*MySQLLedger, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	// database/sql keeps 2 idle connections by default, so a worker running
	// dozens of activities at once would open (and handshake) a new MySQL
	// connection for most queries. Keep enough idle to cover the concurrency.
	db.SetMaxIdleConns(64)
	if err := db.PingContext(ctx); err != nil {
		return nil, errors.Join(err, db.Close())
	}
	if err := migrate(ctx, db); err != nil {
		return nil, errors.Join(err, db.Close())
	}
	return &MySQLLedger{db: db}, nil
}

func (l *MySQLLedger) Close() error { return l.db.Close() }

// Ping checks the database is reachable, for /healthz.
func (l *MySQLLedger) Ping(ctx context.Context) error { return l.db.PingContext(ctx) }

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
	for i, id := range ids {
		args[i] = id
	}
	// Create missing accounts outside the transaction: INSERT IGNORE on an
	// existing row takes a shared lock, and upgrading it to FOR UPDATE inside
	// the transaction would deadlock two concurrent holds on one card.
	if _, err := l.db.ExecContext(ctx, `INSERT IGNORE INTO accounts (id) VALUES `+strings.TrimSuffix(strings.Repeat("(?),", len(ids)), ","), args...); err != nil {
		return false, err
	}

	tx, err := l.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback() }() // no-op after Commit

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
			return false, errors.Join(err, rows.Close())
		}
		balances[id] = b
	}
	// Next returning false closed rows, freeing the connection for the writes below.
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

func (l *MySQLLedger) RecordAttempt(ctx context.Context, card, id string, at time.Time, window time.Duration) (int, error) {
	// The lock row is created outside the transaction, for the same reason
	// PostWithin creates accounts first: INSERT IGNORE on an existing row
	// takes a shared lock that FOR UPDATE would then deadlock upgrading.
	if _, err := l.db.ExecContext(ctx, `INSERT IGNORE INTO attempt_locks (card) VALUES (?)`, card); err != nil {
		return 0, err
	}
	tx, err := l.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback() }() // no-op after Commit
	if _, err := tx.ExecContext(ctx, `SELECT card FROM attempt_locks WHERE card = ? FOR UPDATE`, card); err != nil {
		return 0, err
	}
	// A new attempt lands just after the card's last one, at the earliest; a
	// retry hits IGNORE and keeps its first time.
	if _, err := tx.ExecContext(ctx, `INSERT IGNORE INTO attempts (card, auth_id, at_us)
		SELECT ?, ?, GREATEST(?, COALESCE(MAX(at_us), 0) + 1) FROM attempts WHERE card = ?`, card, id, at.UnixMicro(), card); err != nil {
		return 0, err
	}
	var atUs int64
	if err := tx.QueryRowContext(ctx, `SELECT at_us FROM attempts WHERE card = ? AND auth_id = ?`, card, id).Scan(&atUs); err != nil {
		return 0, err
	}
	var n int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM attempts WHERE card = ? AND at_us <= ? AND at_us > ?`,
		card, atUs, atUs-window.Microseconds()).Scan(&n); err != nil {
		return 0, err
	}
	return n, tx.Commit()
}

func (l *MySQLLedger) Accounts(ctx context.Context) (map[string]int64, error) {
	rows, err := l.db.QueryContext(ctx, `SELECT id, balance FROM accounts`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }() // Err below reports what matters
	out := map[string]int64{}
	for rows.Next() {
		var id string
		var b int64
		if err := rows.Scan(&id, &b); err != nil {
			return nil, err
		}
		out[id] = b
	}
	return out, rows.Err()
}

func (l *MySQLLedger) Balance(ctx context.Context, account string) (int64, error) {
	var b int64
	err := l.db.QueryRowContext(ctx, `SELECT balance FROM accounts WHERE id = ?`, account).Scan(&b)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	return b, err
}

func (l *MySQLLedger) CreditLimit(ctx context.Context, card string) (int64, bool, error) {
	return l.lookup(ctx, `SELECT credit_limit FROM cards WHERE id = ?`, card)
}

func (l *MySQLLedger) RewardRate(ctx context.Context, merchant string) (int64, bool, error) {
	return l.lookup(ctx, `SELECT points_per_dollar FROM partners WHERE merchant = ?`, merchant)
}

func (l *MySQLLedger) APR(ctx context.Context, card string) (int64, bool, error) {
	return l.lookup(ctx, `SELECT apr_bps FROM cards WHERE id = ?`, card)
}

func (l *MySQLLedger) lookup(ctx context.Context, query, key string) (int64, bool, error) {
	var v int64
	err := l.db.QueryRowContext(ctx, query, key).Scan(&v)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, false, nil
	}
	return v, err == nil, err
}

// SetCreditLimit creates or updates a card. A negative limit fails the
// table's CHECK constraint, and a closed card can't be changed: a new limit
// would restart its statements.
func (l *MySQLLedger) SetCreditLimit(ctx context.Context, card string, limit int64) error {
	if status, ok, err := l.CardStatus(ctx, card); err != nil {
		return err
	} else if ok && status == CardClosed {
		return fmt.Errorf("card %q is closed", card)
	}
	_, err := l.db.ExecContext(ctx, `INSERT INTO cards (id, credit_limit) VALUES (?, ?) AS new
		ON DUPLICATE KEY UPDATE credit_limit = IF(cards.status = 'closed', cards.credit_limit, new.credit_limit)`, card, limit)
	return err
}

func (l *MySQLLedger) CardStatus(ctx context.Context, card string) (string, bool, error) {
	var status string
	err := l.db.QueryRowContext(ctx, `SELECT status FROM cards WHERE id = ?`, card).Scan(&status)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	return status, err == nil, err
}

// SetCardStatus freezes, unfreezes or closes a card. Closed is final: the
// UPDATE never touches a closed row, so a race can't reopen one either.
func (l *MySQLLedger) SetCardStatus(ctx context.Context, card, status string) error {
	cur, ok, err := l.CardStatus(ctx, card)
	switch {
	case err != nil:
		return err
	case !ok:
		return fmt.Errorf("no card %q", card)
	case cur == CardClosed:
		return fmt.Errorf("card %q is closed", card)
	}
	_, err = l.db.ExecContext(ctx, `UPDATE cards SET status = ? WHERE id = ? AND status <> 'closed'`, status, card)
	return err
}

// SetRewardRate creates or updates a partner's rate. A negative rate fails
// the table's CHECK constraint.
func (l *MySQLLedger) SetRewardRate(ctx context.Context, merchant string, rate int64) error {
	_, err := l.db.ExecContext(ctx, `INSERT INTO partners (merchant, points_per_dollar) VALUES (?, ?) AS new
		ON DUPLICATE KEY UPDATE points_per_dollar = new.points_per_dollar`, merchant, rate)
	return err
}

// SetAPR changes an existing card's APR, in basis points. A negative APR fails
// the column's CHECK constraint.
func (l *MySQLLedger) SetAPR(ctx context.Context, card string, bps int64) error {
	res, err := l.db.ExecContext(ctx, `UPDATE cards SET apr_bps = ? WHERE id = ?`, bps, card)
	if err != nil {
		return err
	}
	if n, err := res.RowsAffected(); err == nil && n == 0 {
		var exists int
		if err := l.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM cards WHERE id = ?`, card).Scan(&exists); err != nil {
			return err
		}
		if exists == 0 {
			return fmt.Errorf("no card %q: set-limit creates it", card)
		}
	}
	return nil
}
