package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

var (
	runID   = time.Now().UnixNano()
	acctSeq atomic.Int64
)

// forEachLedger runs the contract against the in-memory ledger and, when
// CAS_MYSQL_DSN is set and -short is off, against MySQL. Each run gets a name
// prefix so tests sharing one database never see each other's accounts or keys.
func forEachLedger(t *testing.T, test func(t *testing.T, l Ledger, n func(string) string)) {
	t.Run("mem", func(t *testing.T) {
		test(t, NewMemLedger(), func(s string) string { return s })
	})
	dsn := os.Getenv("CAS_MYSQL_DSN")
	t.Run("mysql", func(t *testing.T) {
		if dsn == "" || testing.Short() {
			t.Skip("set CAS_MYSQL_DSN and drop -short to run against MySQL")
		}
		l, err := OpenMySQL(context.Background(), dsn)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { l.Close() })
		prefix := fmt.Sprintf("t%d-%d:", runID, acctSeq.Add(1))
		test(t, l, func(s string) string { return prefix + s })
	})
}

func balance(t *testing.T, l Ledger, account string) int64 {
	t.Helper()
	b, err := l.Balance(context.Background(), account)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestLedgerPost(t *testing.T) {
	tests := []struct {
		name    string
		entries []Entry // accounts a, b, c
		wantErr error
		wantA   int64 // balance of "a" afterwards
	}{
		{"balanced", []Entry{{"a", 500}, {"b", -500}}, nil, 500},
		{"unbalanced", []Entry{{"a", 500}, {"b", -400}}, ErrUnbalanced, 0},
		{"single leg", []Entry{{"a", 0}}, ErrUnbalanced, 0},
		{"no legs", nil, ErrUnbalanced, 0},
		{"three legs", []Entry{{"a", 300}, {"b", -100}, {"c", -200}}, nil, 300},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			forEachLedger(t, func(t *testing.T, l Ledger, n func(string) string) {
				entries := make([]Entry, len(tt.entries))
				for i, e := range tt.entries {
					entries[i] = Entry{n(e.Account), e.Amount}
				}
				if err := l.Post(context.Background(), n("k"), entries...); !errors.Is(err, tt.wantErr) {
					t.Fatalf("Post err = %v, want %v", err, tt.wantErr)
				}
				if got := balance(t, l, n("a")); got != tt.wantA {
					t.Errorf("Balance(a) = %d, want %d", got, tt.wantA)
				}
				if sum := balance(t, l, n("a")) + balance(t, l, n("b")) + balance(t, l, n("c")); sum != 0 {
					t.Errorf("a+b+c = %d, want 0", sum)
				}
			})
		})
	}
}

func TestLedgerPostIsIdempotent(t *testing.T) {
	forEachLedger(t, func(t *testing.T, l Ledger, n func(string) string) {
		for range 3 {
			if err := l.Post(context.Background(), n("same-key"), Entry{n("a"), 100}, Entry{n("b"), -100}); err != nil {
				t.Fatal(err)
			}
		}
		if got := balance(t, l, n("a")); got != 100 {
			t.Errorf("Balance(a) = %d after 3 posts of one key, want 100", got)
		}
	})
}

func TestLedgerPostWithin(t *testing.T) {
	tests := []struct {
		name   string
		seed   int64 // already in the capped account
		amount int64
		want   bool
		wantH  int64 // capped balance afterwards
	}{
		{"under the limit", 0, 60, true, 60},
		{"exactly the limit", 0, 100, true, 100},
		{"over the limit", 0, 101, false, 0},
		{"over once seeded", 70, 40, false, 70},
		{"fits once seeded", 70, 30, true, 100},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			forEachLedger(t, func(t *testing.T, l Ledger, n func(string) string) {
				ctx := context.Background()
				if err := l.Post(ctx, n("seed"), Entry{n("h"), tt.seed}, Entry{n("otb"), -tt.seed}); err != nil {
					t.Fatal(err)
				}
				ok, err := l.PostWithin(ctx, n("hold"), []string{n("h")}, 100, Entry{n("h"), tt.amount}, Entry{n("otb"), -tt.amount})
				if err != nil {
					t.Fatal(err)
				}
				if ok != tt.want {
					t.Errorf("PostWithin = %v, want %v", ok, tt.want)
				}
				if got := balance(t, l, n("h")); got != tt.wantH {
					t.Errorf("capped balance = %d, want %d", got, tt.wantH)
				}
			})
		})
	}
}

// A replayed key reports success even though re-checking now would fail.
func TestLedgerPostWithinReplay(t *testing.T) {
	forEachLedger(t, func(t *testing.T, l Ledger, n func(string) string) {
		ctx := context.Background()
		for range 2 {
			ok, err := l.PostWithin(ctx, n("hold"), []string{n("h")}, 100, Entry{n("h"), 80}, Entry{n("otb"), -80})
			if err != nil || !ok {
				t.Fatalf("PostWithin = %v, %v; want true, nil", ok, err)
			}
		}
		if got := balance(t, l, n("h")); got != 80 {
			t.Errorf("capped balance = %d after a replay, want 80", got)
		}
	})
}

// The race M1 and M2 left open: concurrent holds on one card must never
// oversubscribe its limit.
func TestLedgerPostWithinConcurrent(t *testing.T) {
	forEachLedger(t, func(t *testing.T, l Ledger, n func(string) string) {
		const tries, each, limit = 20, 10, 100
		var wg sync.WaitGroup
		var accepted atomic.Int64
		errs := make(chan error, tries)
		for i := range tries {
			wg.Go(func() {
				ok, err := l.PostWithin(context.Background(), n(fmt.Sprintf("hold-%d", i)),
					[]string{n("h")}, limit, Entry{n("h"), each}, Entry{n("otb"), -each})
				if err != nil {
					errs <- err
				}
				if ok {
					accepted.Add(1)
				}
			})
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			t.Error(err)
		}
		if got := accepted.Load(); got != limit/each {
			t.Errorf("accepted %d holds, want %d", got, limit/each)
		}
		if got := balance(t, l, n("h")); got != limit {
			t.Errorf("capped balance = %d, want %d", got, limit)
		}
	})
}
