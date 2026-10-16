package main

import (
	"context"
	"errors"
	"maps"
	"slices"
	"sync"
	"time"
)

// Entry is one leg of a double-entry posting, in cents. Debits are positive,
// credits negative; the legs of every posting sum to zero.
type Entry struct {
	Account string
	Amount  int64
}

// ErrUnbalanced rejects a posting whose legs don't sum to zero.
var ErrUnbalanced = errors.New("ledger: posting needs at least two legs summing to zero")

// Ledger is a double-entry ledger. Every posting is atomic and idempotent per
// key: re-posting a key is a no-op, so a retried activity never double-posts.
type Ledger interface {
	Post(ctx context.Context, key string, entries ...Entry) error
	// PostWithin posts only if, after the posting, the capped accounts' balances
	// sum to at most limit. The check and the posting are one atomic step.
	// It reports whether the posting is in the ledger (true for a replayed key).
	PostWithin(ctx context.Context, key string, capped []string, limit int64, entries ...Entry) (bool, error)
	Balance(ctx context.Context, account string) (int64, error)
	// Accounts is every account with its balance, for the audit.
	// ponytail: loads them all at once; page by ID when the ledger outgrows memory.
	Accounts(ctx context.Context) (map[string]int64, error)
	// RecordAttempt notes auth attempt id on card at `at`, once per id, and
	// counts the card's attempts in the window ending at this attempt's first
	// recorded time, this one included. A retry gets the same count.
	RecordAttempt(ctx context.Context, card, id string, at time.Time, window time.Duration) (int, error)
}

func balanced(entries []Entry) bool {
	var sum int64
	for _, e := range entries {
		sum += e.Amount
	}
	return len(entries) >= 2 && sum == 0
}

// accountIDs returns the distinct accounts touched, sorted so every
// transaction locks rows in the same order and can't deadlock another.
func accountIDs(capped []string, entries []Entry) []string {
	ids := slices.Clone(capped)
	for _, e := range entries {
		ids = append(ids, e.Account)
	}
	slices.Sort(ids)
	return slices.Compact(ids)
}

// within reports whether the capped accounts (distinct ids) stay at or under
// limit once the entries are applied to the current balances.
func within(balances map[string]int64, capped []string, limit int64, entries []Entry) bool {
	var used int64
	for _, id := range capped {
		used += balances[id]
	}
	for _, e := range entries {
		if slices.Contains(capped, e.Account) {
			used += e.Amount
		}
	}
	return used <= limit
}

// MemLedger is the in-memory Ledger used by unit tests.
type MemLedger struct {
	mu       sync.Mutex
	balances map[string]int64
	posted   map[string]bool                 // idempotency keys already applied
	attempts map[string]map[string]time.Time // card → auth ID → first seen
}

func NewMemLedger() *MemLedger {
	return &MemLedger{balances: map[string]int64{}, posted: map[string]bool{}, attempts: map[string]map[string]time.Time{}}
}

func (l *MemLedger) RecordAttempt(_ context.Context, card, id string, at time.Time, window time.Duration) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.attempts[card] == nil {
		l.attempts[card] = map[string]time.Time{}
	}
	if first, ok := l.attempts[card][id]; ok {
		at = first
	} else {
		l.attempts[card][id] = at
	}
	n := 0
	for _, t := range l.attempts[card] {
		if t.After(at.Add(-window)) && !t.After(at) {
			n++
		}
	}
	return n, nil
}

func (l *MemLedger) Post(ctx context.Context, key string, entries ...Entry) error {
	_, err := l.PostWithin(ctx, key, nil, 0, entries...)
	return err
}

func (l *MemLedger) PostWithin(_ context.Context, key string, capped []string, limit int64, entries ...Entry) (bool, error) {
	if !balanced(entries) {
		return false, ErrUnbalanced
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.posted[key] {
		return true, nil
	}
	if !within(l.balances, capped, limit, entries) {
		return false, nil
	}
	for _, e := range entries {
		l.balances[e.Account] += e.Amount
	}
	l.posted[key] = true
	return true, nil
}

func (l *MemLedger) Balance(_ context.Context, account string) (int64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.balances[account], nil
}

func (l *MemLedger) Accounts(context.Context) (map[string]int64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return maps.Clone(l.balances), nil
}

// Total sums every balance. A correct ledger always returns 0.
func (l *MemLedger) Total() int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	var t int64
	for _, b := range l.balances {
		t += b
	}
	return t
}
