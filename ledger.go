package main

import (
	"context"
	"errors"
	"sort"
	"sync"
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
	seen := map[string]bool{}
	var ids []string
	for _, id := range capped {
		if !seen[id] {
			seen[id] = true
			ids = append(ids, id)
		}
	}
	for _, e := range entries {
		if !seen[e.Account] {
			seen[e.Account] = true
			ids = append(ids, e.Account)
		}
	}
	sort.Strings(ids)
	return ids
}

// within reports whether the capped accounts stay at or under limit once the
// entries are applied to the current balances.
func within(balances map[string]int64, capped []string, limit int64, entries []Entry) bool {
	isCapped := map[string]bool{}
	var used int64
	for _, id := range capped {
		if !isCapped[id] {
			isCapped[id] = true
			used += balances[id]
		}
	}
	for _, e := range entries {
		if isCapped[e.Account] {
			used += e.Amount
		}
	}
	return used <= limit
}

// MemLedger is the in-memory Ledger used by unit tests.
type MemLedger struct {
	mu       sync.Mutex
	balances map[string]int64
	posted   map[string]bool // idempotency keys already applied
}

func NewMemLedger() *MemLedger {
	return &MemLedger{balances: map[string]int64{}, posted: map[string]bool{}}
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
