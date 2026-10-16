package main

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

const (
	PayUpdate      = "pay"       // a card payment; see Payment
	StatementQuery = "statement" // the current Statement

	DefaultCycle  = 30 * 24 * time.Hour // statement cycle length
	DefaultGrace  = 25 * 24 * time.Hour // payment due this long after close
	DefaultDay    = 24 * time.Hour      // one daily balance sample per day
	DefaultAPRBps = 2499                // 24.99% APR, in basis points, for a card with none set
	MinDueFloor   = 2_500               // cents; the minimum due is at least this...
	MinDuePercent = 2                   // ...or this share of the balance
	LateFee       = 2_500               // cents
)

// StatementWorkflowID is the ID of a card's statement cycle: one per card.
func StatementWorkflowID(card string) string { return "statement:" + card }

// StatementTerms are a card's billing terms. Zero values take the defaults.
type StatementTerms struct {
	CardID string
	Cycle  time.Duration
	Grace  time.Duration
	Day    time.Duration // the daily-balance sampling interval; shrunk by drills
	APRBps int64         // runs before daily-balance interest only; newer runs read the card's APR at each close
}

// Statement is one closed cycle and the payments made against it since.
type Statement struct {
	N        int   // 0 before the first close
	Balance  int64 // posted balance at close, cents; negative is a credit
	MinDue   int64
	Fees     int64 // late fees charged in the cycle this statement closes
	Interest int64 // interest charged at this close
	Paid     int64 // paid since this statement closed
}

// Payment is the payload of the pay update. ID makes a resent payment post once.
type Payment struct {
	ID     string
	Amount int64 // cents
}

// StatementWorkflow runs one cycle per run. Each day it samples the card's
// posted balance; on the due day it charges a late fee if the last statement's
// minimum wasn't paid. At close it charges interest on the average daily
// balance unless the last statement was paid in full, closes a new statement,
// and continues as new with it so history never grows past one cycle.
func StatementWorkflow(ctx workflow.Context, terms StatementTerms, last Statement) error {
	ctx = withLedger(ctx)
	var a *Activities
	closing := false
	seen := map[string]bool{} // payment IDs this cycle, to reject a reuse up front
	// Runs started before payments credited a per-statement paid account
	// replay on PostToCard and count Paid themselves.
	v := workflow.GetVersion(ctx, "paid-from-ledger", workflow.DefaultVersion, 1)
	// Runs started before daily-balance interest replay on the close-timer path.
	daily := workflow.GetVersion(ctx, "daily-balance-interest", workflow.DefaultVersion, 1) != workflow.DefaultVersion

	if err := workflow.SetQueryHandler(ctx, StatementQuery, func() (Statement, error) { return last, nil }); err != nil {
		return err
	}
	if err := workflow.SetUpdateHandlerWithOptions(ctx, PayUpdate,
		func(ctx workflow.Context, p Payment) error {
			seen[p.ID] = true
			if v == workflow.DefaultVersion {
				if err := workflow.ExecuteActivity(withLedger(ctx), a.PostToCard, terms.CardID, "pay:"+p.ID, -p.Amount).Get(ctx, nil); err != nil {
					return err
				}
				last.Paid += p.Amount
				return nil
			}
			var paid int64
			if err := workflow.ExecuteActivity(withLedger(ctx), a.Pay, terms.CardID, last.N, p).Get(ctx, &paid); err != nil {
				return err
			}
			last.Paid = max(last.Paid, paid) // concurrent payments can finish out of order
			return nil
		},
		workflow.UpdateHandlerOptions{Validator: func(_ workflow.Context, p Payment) error {
			switch {
			case closing:
				return errors.New("statement closing; retry shortly")
			case p.ID == "" || seen[p.ID]:
				return fmt.Errorf("payment ID %q empty or already used", p.ID)
			case p.Amount <= 0:
				return fmt.Errorf("payment %d not positive", p.Amount)
			}
			return nil
		}},
	); err != nil {
		return err
	}

	step := func(name string) string { return fmt.Sprintf("s%d:%s", last.N+1, name) }
	next := Statement{N: last.N + 1}
	if daily {
		return closeDaily(ctx, a, terms, &last, &closing, step)
	}

	due := workflow.NewTimer(ctx, cmp.Or(terms.Grace, DefaultGrace))
	closeAt := workflow.NewTimer(ctx, cmp.Or(terms.Cycle, DefaultCycle))
	if err := due.Get(ctx, nil); err != nil {
		return err
	}
	if last.Paid < last.MinDue {
		next.Fees = LateFee
		if err := workflow.ExecuteActivity(ctx, a.PostToCard, terms.CardID, step("late-fee"), next.Fees).Get(ctx, nil); err != nil {
			return err
		}
	}
	if err := closeAt.Get(ctx, nil); err != nil {
		return err
	}
	closing = true
	// Payments already accepted finish before the balance is read.
	if err := workflow.Await(ctx, func() bool { return workflow.AllHandlersFinished(ctx) }); err != nil {
		return err
	}
	if unpaid := last.Balance - last.Paid; unpaid > 0 {
		next.Interest = unpaid * cmp.Or(terms.APRBps, DefaultAPRBps) / 12 / 10_000
		if err := workflow.ExecuteActivity(ctx, a.PostToCard, terms.CardID, step("interest"), next.Interest).Get(ctx, nil); err != nil {
			return err
		}
	}
	if err := workflow.ExecuteActivity(ctx, a.PostedBalance, terms.CardID).Get(ctx, &next.Balance); err != nil {
		return err
	}
	next.MinDue = minDue(next.Balance)
	return workflow.NewContinueAsNewError(ctx, StatementWorkflow, terms, next)
}

// closeDaily is one cycle on daily balances. It runs day by day, so the late
// fee lands before that day's sample and every sample is in a fixed order.
// Its ledger steps retry until the ledger answers: a statement has nothing to
// compensate, and giving up would end the card's billing.
func closeDaily(ctx workflow.Context, a *Activities, terms StatementTerms, last *Statement, closing *bool, step func(string) string) error {
	ctx = workflow.WithRetryPolicy(ctx, temporal.RetryPolicy{})
	day := cmp.Or(terms.Day, DefaultDay)
	days := max(int(cmp.Or(terms.Cycle, DefaultCycle)/day), 1)
	dueDay := min(int((cmp.Or(terms.Grace, DefaultGrace)+day-1)/day), days) // the first day end at or after the grace period
	next := Statement{N: last.N + 1}

	var dailySum int64 // Σ end-of-day posted balance, credits counted as 0
	for d := 1; d <= days; d++ {
		if err := workflow.Sleep(ctx, day); err != nil {
			return err
		}
		if d == dueDay && last.Paid < last.MinDue {
			next.Fees = LateFee
			if err := workflow.ExecuteActivity(ctx, a.PostToCard, terms.CardID, step("late-fee"), next.Fees).Get(ctx, nil); err != nil {
				return err
			}
		}
		var b int64
		if err := workflow.ExecuteActivity(ctx, a.PostedBalance, terms.CardID).Get(ctx, &b); err != nil {
			return err
		}
		dailySum += max(b, 0)
	}

	*closing = true
	// Payments already accepted finish before interest and the balance.
	if err := workflow.Await(ctx, func() bool { return workflow.AllHandlersFinished(ctx) }); err != nil {
		return err
	}
	// Grace: a statement paid in full by the next close accrues nothing, and
	// the first cycle has no statement to carry a balance from.
	if last.N > 0 && last.Paid < last.Balance {
		var apr int64
		if err := workflow.ExecuteActivity(ctx, a.CardAPR, terms.CardID).Get(ctx, &apr); err != nil {
			return err
		}
		next.Interest = dailySum * apr / (365 * 10_000) // daily periodic rate × each day's balance
		if next.Interest > 0 {
			if err := workflow.ExecuteActivity(ctx, a.PostToCard, terms.CardID, step("interest"), next.Interest).Get(ctx, nil); err != nil {
				return err
			}
		}
	}
	if err := workflow.ExecuteActivity(ctx, a.PostedBalance, terms.CardID).Get(ctx, &next.Balance); err != nil {
		return err
	}
	next.MinDue = minDue(next.Balance)
	// A closed card's statements end once nothing is posted or held. Runs from
	// before closing existed always continue.
	if workflow.GetVersion(ctx, "end-when-closed", workflow.DefaultVersion, 1) != workflow.DefaultVersion {
		var done bool
		if err := workflow.ExecuteActivity(ctx, a.CardDone, terms.CardID).Get(ctx, &done); err != nil {
			return err
		}
		if done {
			workflow.GetLogger(ctx).Info("closed card paid off: statements end", "card", terms.CardID, "statement", next.N)
			return nil
		}
	}
	return workflow.NewContinueAsNewError(ctx, StatementWorkflow, terms, next)
}

// CardDone reports whether a card is closed with nothing posted or held and
// no auth still running on it, so its statements can stop.
func (a *Activities) CardDone(ctx context.Context, card string) (bool, error) {
	status, _, err := a.Config.CardStatus(ctx, card)
	if err != nil || status != CardClosed {
		return false, err
	}
	for _, account := range []string{postedAccount(card), holdsAccount(card)} {
		b, err := a.Ledger.Balance(ctx, account)
		if err != nil || b != 0 {
			return false, err
		}
	}
	if a.Temporal == nil { // unit tests: no visibility to ask
		return true, nil
	}
	// An auth still running could post a refund or chargeback after the final
	// statement; so could one from before CardID tagging, which can't be seen.
	for _, count := range []func(context.Context) (int64, error){
		func(ctx context.Context) (int64, error) { return a.runningAuths(ctx, card) },
		a.untaggedAuths,
	} {
		if n, err := count(ctx); err != nil || n > 0 {
			return false, err
		}
	}
	return true, nil
}

// minDue is the larger of the floor and a share of the balance, but never more
// than the balance; nothing is due on a credit.
func minDue(balance int64) int64 {
	return min(max(MinDueFloor, balance*MinDuePercent/100), max(balance, 0))
}

// PostToCard moves amount into the card's posted balance from open-to-buy:
// positive for a fee or interest, negative for a payment.
func (a *Activities) PostToCard(ctx context.Context, card, step string, amount int64) error {
	return a.Ledger.Post(ctx, stepKey(ctx, step),
		Entry{postedAccount(card), amount},
		Entry{openToBuyAccount(card), -amount},
	)
}

// Pay posts a payment against statement n and returns what n has been paid
// so far. The posting is keyed by the payment ID alone and also credits n's
// paid account, so a resend in any later cycle posts nothing and leaves that
// cycle's paid total alone.
func (a *Activities) Pay(ctx context.Context, card string, n int, p Payment) (int64, error) {
	paid := fmt.Sprintf("card:%s:paid:s%d", card, n)
	if err := a.Ledger.Post(ctx, stepKey(ctx, "pay:"+p.ID),
		Entry{postedAccount(card), -p.Amount},
		Entry{openToBuyAccount(card), p.Amount},
		Entry{paid, p.Amount},                          // memo: what statement n has been paid,
		Entry{"card:" + card + ":payments", -p.Amount}, // balanced by all payments made
	); err != nil {
		return 0, err
	}
	return a.Ledger.Balance(ctx, paid)
}

// CardAPR is the card's APR in basis points, read at each close so a change
// applies from the next statement.
func (a *Activities) CardAPR(ctx context.Context, card string) (int64, error) {
	bps, ok, err := a.Config.APR(ctx, card)
	if !ok && err == nil {
		bps = DefaultAPRBps
	}
	return bps, err
}

// PostedBalance is the card's posted balance, which a statement closes on.
func (a *Activities) PostedBalance(ctx context.Context, card string) (int64, error) {
	return a.Ledger.Balance(ctx, postedAccount(card))
}
