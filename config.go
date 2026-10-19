package main

import (
	"cmp"
	"context"
)

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
	// CardAccount is the account a card number belongs to, and whether that
	// number has been replaced (SPEC M18). A number with no record is its own
	// account, so an unknown number reaches the limit check and declines there.
	CardAccount(ctx context.Context, number string) (account string, replaced bool, err error)
	// CurrentNumber is the number its card carries now: the number itself, or
	// the one that replaced it (SPEC M19's account updater).
	CurrentNumber(ctx context.Context, number string) (string, error)
	// MerchantBlocked reports whether the card's account has blocked the merchant (SPEC M22).
	MerchantBlocked(ctx context.Context, account, merchant string) (bool, error)
}

// Card statuses (SPEC M16). Closed is final.
const (
	CardActive = "active"
	CardFrozen = "frozen"
	CardClosed = "closed"
)

// StaticConfig is a fixed Config, for tests and the in-memory worker.
type StaticConfig struct {
	Limits   map[string]int64   // card → credit limit, cents
	Rates    map[string]int64   // merchant → points per $1
	APRs     map[string]int64   // card → APR, basis points
	Statuses map[string]string  // card → status; missing is active
	Numbers  map[string]string  // replacement number → account; missing is its own account
	Replaced map[string]bool    // numbers that have been replaced
	Blocked  map[[2]string]bool // {account, merchant} pairs the cardholder blocked
}

func (c StaticConfig) MerchantBlocked(_ context.Context, account, merchant string) (bool, error) {
	return c.Blocked[[2]string{account, merchant}], nil
}

func (c StaticConfig) CardAccount(_ context.Context, number string) (string, bool, error) {
	return cmp.Or(c.Numbers[number], number), c.Replaced[number], nil
}

func (c StaticConfig) CurrentNumber(_ context.Context, number string) (string, error) {
	account := cmp.Or(c.Numbers[number], number)
	for n, a := range c.Numbers {
		if a == account && !c.Replaced[n] {
			return n, nil
		}
	}
	return number, nil // never replaced
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
