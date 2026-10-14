package main

import "context"

// Config is the per-card and per-partner configuration the activities read.
// The MySQL ledger serves it from the cards and partners tables.
type Config interface {
	// CreditLimit is the card's limit in cents; ok is false for an unknown card.
	CreditLimit(ctx context.Context, card string) (limit int64, ok bool, err error)
	// RewardRate is the partner's points per $1; ok is false for a merchant
	// with no partner rate.
	RewardRate(ctx context.Context, merchant string) (rate int64, ok bool, err error)
}

// StaticConfig is a fixed Config, for tests and the in-memory worker.
type StaticConfig struct {
	Limits map[string]int64 // card → credit limit, cents
	Rates  map[string]int64 // merchant → points per $1
}

func (c StaticConfig) CreditLimit(_ context.Context, card string) (int64, bool, error) {
	limit, ok := c.Limits[card]
	return limit, ok, nil
}

func (c StaticConfig) RewardRate(_ context.Context, merchant string) (int64, bool, error) {
	rate, ok := c.Rates[merchant]
	return rate, ok, nil
}
