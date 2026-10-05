package main

import (
	"errors"
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

// Ledger is an in-memory double-entry ledger.
// ponytail: in-memory only; M3 swaps in MySQL behind the same Post/Balance surface.
type Ledger struct {
	mu       sync.Mutex
	balances map[string]int64
	posted   map[string]bool // idempotency keys already applied
}

func NewLedger() *Ledger {
	return &Ledger{balances: map[string]int64{}, posted: map[string]bool{}}
}

// Post applies entries atomically under key. Re-posting a key is a no-op, so an
// activity retry never double-posts.
func (l *Ledger) Post(key string, entries ...Entry) error {
	var sum int64
	for _, e := range entries {
		sum += e.Amount
	}
	if len(entries) < 2 || sum != 0 {
		return ErrUnbalanced
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.posted[key] {
		return nil
	}
	for _, e := range entries {
		l.balances[e.Account] += e.Amount
	}
	l.posted[key] = true
	return nil
}

func (l *Ledger) Balance(account string) int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.balances[account]
}

// Total sums every balance. A correct ledger always returns 0.
func (l *Ledger) Total() int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	var t int64
	for _, b := range l.balances {
		t += b
	}
	return t
}
