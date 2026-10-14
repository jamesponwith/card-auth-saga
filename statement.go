package main

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"time"

	"go.temporal.io/sdk/workflow"
)

const (
	PayUpdate      = "pay"       // a card payment; see Payment
	StatementQuery = "statement" // the current Statement

	DefaultCycle  = 30 * 24 * time.Hour // statement cycle length
	DefaultGrace  = 25 * 24 * time.Hour // payment due this long after close
	DefaultAPRBps = 2499                // 24.99% APR, in basis points
	MinDueFloor   = 2_500               // cents; the minimum due is at least this...
	MinDuePercent = 2                   // ...or this share of the balance
	LateFee       = 2_500               // cents
)

// StatementTerms are a card's billing terms. Zero values take the defaults.
type StatementTerms struct {
	CardID string
	Cycle  time.Duration
	Grace  time.Duration
	APRBps int64
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

// StatementWorkflow runs one cycle per run: from the last close it charges a
// late fee at the due date if the minimum wasn't paid, then at the next close
// charges interest on what's left of the last statement, closes a new one, and
// continues as new with it so history never grows past one cycle.
func StatementWorkflow(ctx workflow.Context, terms StatementTerms, last Statement) error {
	ctx = withLedger(ctx)
	var a *Activities
	closing := false
	// ponytail: payment IDs dedupe per cycle. The ledger key ("pay:"+ID) stops a
	// resend in a later cycle from posting twice, but it would still count toward
	// that cycle's Paid; carry IDs forward or have Post report duplicates if so.
	seen := map[string]bool{}

	if err := workflow.SetQueryHandler(ctx, StatementQuery, func() (Statement, error) { return last, nil }); err != nil {
		return err
	}
	if err := workflow.SetUpdateHandlerWithOptions(ctx, PayUpdate,
		func(ctx workflow.Context, p Payment) error {
			seen[p.ID] = true
			if err := workflow.ExecuteActivity(withLedger(ctx), a.PostToCard, terms.CardID, "pay:"+p.ID, -p.Amount).Get(ctx, nil); err != nil {
				return err
			}
			last.Paid += p.Amount
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
	// ponytail: interest on the unpaid statement balance at close, not an
	// average daily balance; switch if interest must match a real issuer.
	if unpaid := last.Balance - last.Paid; unpaid > 0 {
		next.Interest = unpaid * cmp.Or(terms.APRBps, DefaultAPRBps) / 12 / 10_000
		if err := workflow.ExecuteActivity(ctx, a.PostToCard, terms.CardID, step("interest"), next.Interest).Get(ctx, nil); err != nil {
			return err
		}
	}
	if err := workflow.ExecuteActivity(ctx, a.PostedBalance, terms.CardID).Get(ctx, &next.Balance); err != nil {
		return err
	}
	next.MinDue = min(max(MinDueFloor, next.Balance*MinDuePercent/100), max(next.Balance, 0))
	return workflow.NewContinueAsNewError(ctx, StatementWorkflow, terms, next)
}

// PostToCard moves amount into the card's posted balance from open-to-buy:
// positive for a fee or interest, negative for a payment.
func (a *Activities) PostToCard(ctx context.Context, card, step string, amount int64) error {
	return a.Ledger.Post(ctx, stepKey(ctx, step),
		Entry{postedAccount(card), amount},
		Entry{openToBuyAccount(card), -amount},
	)
}

// PostedBalance is the card's posted balance, which a statement closes on.
func (a *Activities) PostedBalance(ctx context.Context, card string) (int64, error) {
	return a.Ledger.Balance(ctx, postedAccount(card))
}
