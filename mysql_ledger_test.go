package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
)

var freshDBs atomic.Int64

// freshDB creates an empty database for one test and returns its DSN. It skips
// unless CAS_MYSQL_DSN is set and -short is off.
func freshDB(t *testing.T) string {
	t.Helper()
	dsn := os.Getenv("CAS_MYSQL_DSN")
	if dsn == "" || testing.Short() {
		t.Skip("set CAS_MYSQL_DSN and drop -short to run against MySQL")
	}
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		t.Fatal(err)
	}
	cfg.DBName = ""
	admin, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = admin.Close() })
	cfg.DBName = fmt.Sprintf("cas_test_%d_%d", runID%1_000_000_000, freshDBs.Add(1))
	if _, err := admin.Exec("CREATE DATABASE " + cfg.DBName); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = admin.Exec("DROP DATABASE " + cfg.DBName) })
	return cfg.FormatDSN()
}

func open(t *testing.T, dsn string) *MySQLLedger {
	t.Helper()
	l, err := OpenMySQL(context.Background(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	return l
}

func exec(t *testing.T, dsn string, stmts ...string) {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, s := range stmts {
		if _, err := db.Exec(s); err != nil {
			t.Fatalf("%s: %v", s, err)
		}
	}
}

// versions lists the recorded migrations, in order.
func versions(t *testing.T, dsn string) []int {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	rows, err := db.Query("SELECT version FROM schema_migrations ORDER BY version")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var vs []int
	for rows.Next() {
		var v int
		if err := rows.Scan(&v); err != nil {
			t.Fatal(err)
		}
		vs = append(vs, v)
	}
	return vs
}

func allVersions() []int {
	vs := make([]int, len(migrations))
	for i := range vs {
		vs[i] = i + 1
	}
	return vs
}

// The schema as the binary created it before versioned migrations: on boot,
// with CREATE TABLE IF NOT EXISTS. Frozen here so adoption stays tested.
var (
	preVersioningLedger = []string{
		`CREATE TABLE accounts (id VARCHAR(191) PRIMARY KEY, balance BIGINT NOT NULL DEFAULT 0)`,
		`CREATE TABLE postings (idem_key VARCHAR(191) PRIMARY KEY, created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP)`,
		`CREATE TABLE entries (id BIGINT AUTO_INCREMENT PRIMARY KEY, idem_key VARCHAR(191) NOT NULL, account VARCHAR(191) NOT NULL,
			amount BIGINT NOT NULL, FOREIGN KEY (idem_key) REFERENCES postings (idem_key))`,
	}
	preVersioningAttempts = `CREATE TABLE attempts (card VARCHAR(191) NOT NULL, auth_id VARCHAR(191) NOT NULL, at_us BIGINT NOT NULL,
		PRIMARY KEY (card, auth_id), INDEX (card, at_us))`
)

func TestMigrate(t *testing.T) {
	tests := []struct {
		name   string
		before []string // schema and data already in the database
	}{
		{"fresh database", nil},
		{"adopts the original ledger schema", append(slices.Clone(preVersioningLedger),
			`INSERT INTO accounts (id, balance) VALUES ('a', 700), ('b', -700)`,
			`INSERT INTO postings (idem_key) VALUES ('k')`)},
		{"adopts the schema with velocity attempts", append(slices.Clone(preVersioningLedger), preVersioningAttempts,
			`INSERT INTO accounts (id, balance) VALUES ('a', 700), ('b', -700)`,
			`INSERT INTO postings (idem_key) VALUES ('k')`)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dsn := freshDB(t)
			exec(t, dsn, tt.before...)
			l := open(t, dsn)

			if got := versions(t, dsn); fmt.Sprint(got) != fmt.Sprint(allVersions()) {
				t.Fatalf("recorded versions = %v, want %v", got, allVersions())
			}
			ctx := context.Background()
			if tt.before != nil { // the data survived, and the old idempotency key still holds
				if got := balance(t, l, "a"); got != 700 {
					t.Errorf("balance(a) = %d after migrating, want 700", got)
				}
				if err := l.Post(ctx, "k", Entry{"a", 1}, Entry{"b", -1}); err != nil || balance(t, l, "a") != 700 {
					t.Errorf("re-posting an existing key changed balances (err %v)", err)
				}
			}
			// Every table the code uses works.
			if err := l.Post(ctx, "new", Entry{"c", 5}, Entry{"d", -5}); err != nil {
				t.Error(err)
			}
			if _, err := l.RecordAttempt(ctx, "card", "auth", time.Now(), VelocityWindow); err != nil {
				t.Error(err)
			}
			if err := l.SetCreditLimit(ctx, "card", 100); err != nil {
				t.Error(err)
			}
			if err := l.SetRewardRate(ctx, "shop", 2); err != nil {
				t.Error(err)
			}
		})
	}
}

// DDL commits as it goes, so a migration that died halfway is finished by
// running it again: every migration must be safe to re-apply.
func TestMigrationsAreRerunnable(t *testing.T) {
	dsn := freshDB(t)
	l := open(t, dsn)
	if err := l.SetCreditLimit(context.Background(), "card", 100); err != nil {
		t.Fatal(err)
	}
	exec(t, dsn, "DELETE FROM schema_migrations") // as if no migration had been recorded

	l2 := open(t, dsn)

	if got := versions(t, dsn); fmt.Sprint(got) != fmt.Sprint(allVersions()) {
		t.Errorf("recorded versions = %v, want %v", got, allVersions())
	}
	if limit, ok, err := l2.CreditLimit(context.Background(), "card"); err != nil || !ok || limit != 100 {
		t.Errorf("CreditLimit after re-running migrations = %d, %v, %v; want 100", limit, ok, err)
	}
}

// Workers booting together migrate once; the rest wait on the lock and find
// nothing left to do.
func TestMigrateConcurrentBoots(t *testing.T) {
	dsn := freshDB(t)
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			l, err := OpenMySQL(context.Background(), dsn)
			if err != nil {
				t.Error(err)
				return
			}
			_ = l.Close()
		})
	}
	wg.Wait()
	if got := versions(t, dsn); fmt.Sprint(got) != fmt.Sprint(allVersions()) {
		t.Errorf("recorded versions = %v, want %v", got, allVersions())
	}
}

// A running worker keeps its connections, so the migrate lock must be released,
// not left on a pooled session, or the next worker's boot would hang.
func TestMigrateReleasesLock(t *testing.T) {
	dsn := freshDB(t)
	open(t, dsn) // stays open, like a running worker
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	l, err := OpenMySQL(ctx, dsn)
	if err != nil {
		t.Fatalf("second boot while the first is running: %v", err)
	}
	_ = l.Close()
}

// A boot that can't get the lock must not migrate unguarded.
func TestMigrateWaitsForTheLock(t *testing.T) {
	dsn := freshDB(t)
	old := migrateLockWait
	migrateLockWait = time.Second
	t.Cleanup(func() { migrateLockWait = old })
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	holder, err := db.Conn(context.Background()) // another boot, mid-migration
	if err != nil {
		t.Fatal(err)
	}
	defer holder.Close()
	if _, err := holder.ExecContext(context.Background(), "DO GET_LOCK(?, 0)", migrateLock); err != nil {
		t.Fatal(err)
	}

	if _, err := OpenMySQL(context.Background(), dsn); err == nil || !strings.Contains(err.Error(), "lock") {
		t.Errorf("OpenMySQL while another boot holds the lock: %v, want a lock timeout", err)
	}
	var tables int
	if err := db.QueryRow("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE()").Scan(&tables); err != nil {
		t.Fatal(err)
	}
	if tables != 0 {
		t.Errorf("created %d tables without the lock, want none", tables)
	}
}

// An old binary must not run against a schema it doesn't know.
func TestMigrateRefusesNewerDatabase(t *testing.T) {
	dsn := freshDB(t)
	open(t, dsn)
	exec(t, dsn, fmt.Sprintf("INSERT INTO schema_migrations (version) VALUES (%d)", len(migrations)+1))

	_, err := OpenMySQL(context.Background(), dsn)
	if err == nil || !strings.Contains(err.Error(), "newer") {
		t.Errorf("OpenMySQL on a newer database: %v, want a refusal", err)
	}
}

func TestMySQLConfig(t *testing.T) {
	l := open(t, freshDB(t))
	ctx := context.Background()
	if _, ok, err := l.CreditLimit(ctx, "card-1"); ok || err != nil {
		t.Errorf("unknown card: ok %v, err %v; want not found", ok, err)
	}
	if _, ok, err := l.RewardRate(ctx, "shop"); ok || err != nil {
		t.Errorf("unknown merchant: ok %v, err %v; want not found", ok, err)
	}
	if _, ok, err := l.APR(ctx, "card-1"); ok || err != nil {
		t.Errorf("unknown card APR: ok %v, err %v; want not found", ok, err)
	}
	steps := []struct {
		name    string
		set     func() error
		limit   int64 // card-1 afterwards
		rate    int64 // shop afterwards
		wantErr bool
	}{
		{"set a limit", func() error { return l.SetCreditLimit(ctx, "card-1", 100_000) }, 100_000, -1, false},
		{"raise it", func() error { return l.SetCreditLimit(ctx, "card-1", 150_000) }, 150_000, -1, false},
		{"zero is a limit", func() error { return l.SetCreditLimit(ctx, "card-1", 0) }, 0, -1, false},
		{"negative limit rejected", func() error { return l.SetCreditLimit(ctx, "card-1", -1) }, 0, -1, true},
		{"set a rate", func() error { return l.SetRewardRate(ctx, "shop", 3) }, 0, 3, false},
		{"zero is a rate", func() error { return l.SetRewardRate(ctx, "shop", 0) }, 0, 0, false},
		{"negative rate rejected", func() error { return l.SetRewardRate(ctx, "shop", -2) }, 0, 0, true},
	}
	for _, st := range steps {
		if err := st.set(); (err != nil) != st.wantErr {
			t.Fatalf("%s: err = %v, wantErr %v", st.name, err, st.wantErr)
		}
		if limit, ok, err := l.CreditLimit(ctx, "card-1"); err != nil || !ok || limit != st.limit {
			t.Errorf("%s: CreditLimit = %d, %v, %v; want %d", st.name, limit, ok, err, st.limit)
		}
		if st.rate >= 0 {
			if rate, ok, err := l.RewardRate(ctx, "shop"); err != nil || !ok || rate != st.rate {
				t.Errorf("%s: RewardRate = %d, %v, %v; want %d", st.name, rate, ok, err, st.rate)
			}
		}
	}

	// A new card gets the default APR; it can be changed, never made negative.
	for _, st := range []struct {
		set     int64 // -1: don't set
		want    int64
		wantErr bool
	}{{-1, DefaultAPRBps, false}, {1_999, 1_999, false}, {0, 0, false}, {-5, 0, true}} {
		if st.set != -1 || st.wantErr {
			if err := l.SetAPR(ctx, "card-1", st.set); (err != nil) != st.wantErr {
				t.Fatalf("SetAPR(%d): err = %v, wantErr %v", st.set, err, st.wantErr)
			}
		}
		if bps, ok, err := l.APR(ctx, "card-1"); err != nil || !ok || bps != st.want {
			t.Errorf("after SetAPR(%d): APR = %d, %v, %v; want %d", st.set, bps, ok, err, st.want)
		}
	}
	if err := l.SetAPR(ctx, "no-such-card", 1_000); err == nil {
		t.Error("SetAPR on a missing card succeeded")
	}
}

// Status moves active ⇄ frozen and either way to closed; closed is final, and
// a closed card's limit can't be set (it would restart its statements).
func TestCardStatusTransitions(t *testing.T) {
	l := open(t, freshDB(t))
	ctx := context.Background()
	if err := l.SetCreditLimit(ctx, "card-1", 100_000); err != nil {
		t.Fatal(err)
	}
	steps := []struct {
		to      string
		want    string // status afterwards
		wantErr bool
	}{
		{"", CardActive, false}, // a new card is active
		{CardFrozen, CardFrozen, false},
		{CardFrozen, CardFrozen, false}, // freezing twice is fine
		{CardActive, CardActive, false},
		{CardClosed, CardClosed, false},
		{CardActive, CardClosed, true}, // closed is final
		{CardFrozen, CardClosed, true},
	}
	for _, st := range steps {
		if st.to != "" {
			if err := l.SetCardStatus(ctx, "card-1", st.to); (err != nil) != st.wantErr {
				t.Fatalf("SetCardStatus(%s): err = %v, wantErr %v", st.to, err, st.wantErr)
			}
		}
		if got, ok, err := l.CardStatus(ctx, "card-1"); err != nil || !ok || got != st.want {
			t.Errorf("after %q: status = %q, %v, %v; want %q", st.to, got, ok, err, st.want)
		}
	}
	if err := l.SetCreditLimit(ctx, "card-1", 200_000); err == nil {
		t.Error("set a closed card's limit")
	}
	if err := l.SetCardStatus(ctx, "no-such-card", CardFrozen); err == nil {
		t.Error("froze a card that doesn't exist")
	}
	if err := l.SetCreditLimit(ctx, "card-2", 1_000); err != nil {
		t.Fatal(err)
	}
	if err := l.SetCardStatus(ctx, "card-2", "lost"); err == nil { // not a status: the CHECK rejects it
		t.Error("set an active card's status to \"lost\"")
	}
}
