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
	// APR is the card's annual rate in basis points; ok is false for an unknown card.
	APR(ctx context.Context, card string) (bps int64, ok bool, err error)
	// CardStatus is CardActive, CardFrozen or CardClosed; ok is false for an unknown card.
	CardStatus(ctx context.Context, card string) (status string, ok bool, err error)
}

// Card statuses (SPEC M16). Closed is final.
const (
	CardActive = "active"
	CardFrozen = "frozen"
	CardClosed = "closed"
)

// StaticConfig is a fixed Config, for tests and the in-memory worker.
type StaticConfig struct {
	Limits   map[string]int64  // card → credit limit, cents
	Rates    map[string]int64  // merchant → points per $1
	APRs     map[string]int64  // card → APR, basis points
	Statuses map[string]string // card → status; missing is active
}

func (c StaticConfig) CreditLimit(_ context.Context, card string) (int64, bool, error) {
	limit, ok := c.Limits[card]
	return limit, ok, nil
}

func (c StaticConfig) RewardRate(_ context.Context, merchant string) (int64, bool, error) {
	rate, ok := c.Rates[merchant]
	return rate, ok, nil
}

func (c StaticConfig) APR(_ context.Context, card string) (int64, bool, error) {
	bps, ok := c.APRs[card]
	return bps, ok, nil
}

func (c StaticConfig) CardStatus(_ context.Context, card string) (string, bool, error) {
	if s, ok := c.Statuses[card]; ok {
		return s, true, nil
	}
	return CardActive, true, nil
}
