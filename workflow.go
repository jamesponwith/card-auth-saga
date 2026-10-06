package main

import (
	"cmp"
	"context"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

const (
	TaskQueue           = "card-auth"
	CaptureSignal       = "capture"
	ReverseSignal       = "reverse"
	RefundSignal        = "refund"
	DefaultHoldFor      = 7 * 24 * time.Hour  // how long an uncaptured hold lives
	DefaultRefundWindow = 30 * 24 * time.Hour // how long after capture refunds are accepted
)

// Workflow outcomes.
const (
	StatusDeclined = "declined"
	StatusExpired  = "expired"
	StatusReversed = "reversed" // merchant voided the auth before capture
	StatusCaptured = "captured" // possibly partially refunded; see Refunded
	StatusRefunded = "refunded" // fully refunded
)

type AuthRequest struct {
	CardID       string
	Amount       int64 // cents
	Merchant     string
	HoldFor      time.Duration // zero means DefaultHoldFor
	RefundWindow time.Duration // zero means DefaultRefundWindow
}

// Capture is the payload of the capture signal. Amount may be less than the
// hold (partial capture); the remainder is released.
type Capture struct {
	Amount int64 // cents
}

// Refund is the payload of the refund signal. ID makes a resent refund a no-op.
type Refund struct {
	ID     string
	Amount int64 // cents
}

type AuthResult struct {
	Status   string
	Captured int64 // cents posted to the card
	Refunded int64 // cents refunded after capture
}

// Card accounts. A hold moves money from open-to-buy into holds; a capture
// moves it from holds into posted and releases any remainder; a refund moves
// it from posted back to open-to-buy.
func holdsAccount(card string) string     { return "card:" + card + ":holds" }
func postedAccount(card string) string    { return "card:" + card + ":posted" }
func openToBuyAccount(card string) string { return "card:" + card + ":open-to-buy" }

// AuthorizeWorkflow places a hold if the card's limit allows it, then waits for
// a capture, a reversal, or expiry. After a capture it accepts refunds until
// the refund window closes or the capture is fully refunded.
func AuthorizeWorkflow(ctx workflow.Context, req AuthRequest) (AuthResult, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Second,
		// Bounded so a ledger that keeps failing surfaces here, where the saga
		// can compensate, instead of retrying forever.
		RetryPolicy: &temporal.RetryPolicy{MaximumAttempts: 5},
	})
	var a *Activities

	var held bool
	if err := workflow.ExecuteActivity(ctx, a.PlaceHold, req).Get(ctx, &held); err != nil {
		return AuthResult{}, err
	}
	if !held {
		return AuthResult{Status: StatusDeclined}, nil
	}

	captured, status, err := awaitCapture(ctx, a, req)
	if err != nil || captured == 0 {
		return AuthResult{Status: status}, err
	}
	refunded, err := acceptRefunds(ctx, a, req, captured)
	if err != nil {
		return AuthResult{}, err
	}
	res := AuthResult{Status: StatusCaptured, Captured: captured, Refunded: refunded}
	if refunded == captured {
		res.Status = StatusRefunded
	}
	return res, nil
}

// awaitCapture waits for a valid capture, a reversal, or the hold to expire.
// It returns the captured amount (0 if the hold was released) and the status.
func awaitCapture(ctx workflow.Context, a *Activities, req AuthRequest) (int64, string, error) {
	expiry := workflow.NewTimer(ctx, cmp.Or(req.HoldFor, DefaultHoldFor))
	captures := workflow.GetSignalChannel(ctx, CaptureSignal)
	reversals := workflow.GetSignalChannel(ctx, ReverseSignal)

	for {
		var c Capture
		release := ""
		sel := workflow.NewSelector(ctx)
		sel.AddReceive(captures, func(ch workflow.ReceiveChannel, _ bool) { ch.Receive(ctx, &c) })
		sel.AddReceive(reversals, func(ch workflow.ReceiveChannel, _ bool) { ch.Receive(ctx, nil); release = StatusReversed })
		sel.AddFuture(expiry, func(workflow.Future) { release = StatusExpired })
		sel.Select(ctx)

		if release != "" {
			return 0, release, workflow.ExecuteActivity(ctx, a.ReleaseHold, req).Get(ctx, nil)
		}
		if c.Amount <= 0 || c.Amount > req.Amount {
			workflow.GetLogger(ctx).Warn("ignoring invalid capture", "amount", c.Amount, "hold", req.Amount)
			continue
		}
		if err := workflow.ExecuteActivity(ctx, a.CaptureHold, req, c.Amount).Get(ctx, nil); err != nil {
			// Compensate: a capture that can't post must not leave the hold
			// eating the card's limit.
			if rerr := workflow.ExecuteActivity(ctx, a.ReleaseHold, req).Get(ctx, nil); rerr != nil {
				workflow.GetLogger(ctx).Error("compensating release failed", "error", rerr)
			}
			return 0, "", err
		}
		return c.Amount, StatusCaptured, nil
	}
}

// acceptRefunds posts refunds until the window closes or the capture is fully
// refunded, and returns the total refunded.
func acceptRefunds(ctx workflow.Context, a *Activities, req AuthRequest, captured int64) (int64, error) {
	window := workflow.NewTimer(ctx, cmp.Or(req.RefundWindow, DefaultRefundWindow))
	refunds := workflow.GetSignalChannel(ctx, RefundSignal)
	seen := map[string]bool{}
	var refunded int64

	for refunded < captured {
		var r Refund
		closed := false
		sel := workflow.NewSelector(ctx)
		sel.AddReceive(refunds, func(ch workflow.ReceiveChannel, _ bool) { ch.Receive(ctx, &r) })
		sel.AddFuture(window, func(workflow.Future) { closed = true })
		sel.Select(ctx)

		if closed {
			break
		}
		if r.ID == "" || seen[r.ID] || r.Amount <= 0 || refunded+r.Amount > captured {
			workflow.GetLogger(ctx).Warn("ignoring refund", "id", r.ID, "amount", r.Amount, "refundable", captured-refunded)
			continue
		}
		if err := workflow.ExecuteActivity(ctx, a.RefundCapture, req, r).Get(ctx, nil); err != nil {
			return refunded, err
		}
		seen[r.ID] = true
		refunded += r.Amount
	}
	return refunded, nil
}

// Activities do the I/O the workflow can't: every ledger read and write.
// Each posting is keyed by "<workflow ID>:<step>", so a retried activity posts once.
type Activities struct {
	Ledger Ledger
	Limits map[string]int64 // card ID → credit limit, cents
}

func stepKey(ctx context.Context, step string) string {
	return activity.GetInfo(ctx).WorkflowExecution.ID + ":" + step
}

// PlaceHold moves the amount from open-to-buy into holds if holds + posted stay
// within the card's limit. The check and the hold are one atomic ledger step,
// so concurrent auths on one card can't oversubscribe it.
func (a *Activities) PlaceHold(ctx context.Context, req AuthRequest) (bool, error) {
	if req.Amount <= 0 {
		return false, temporal.NewNonRetryableApplicationError("amount must be positive", "InvalidAmount", nil)
	}
	limit, known := a.Limits[req.CardID]
	if !known {
		return false, nil
	}
	return a.Ledger.PostWithin(ctx, stepKey(ctx, "hold"),
		[]string{holdsAccount(req.CardID), postedAccount(req.CardID)}, limit,
		Entry{holdsAccount(req.CardID), req.Amount},
		Entry{openToBuyAccount(req.CardID), -req.Amount},
	)
}

// CaptureHold clears the hold, posts the captured amount, and releases the rest.
func (a *Activities) CaptureHold(ctx context.Context, req AuthRequest, amount int64) error {
	return a.Ledger.Post(ctx, stepKey(ctx, "capture"),
		Entry{holdsAccount(req.CardID), -req.Amount},
		Entry{postedAccount(req.CardID), amount},
		Entry{openToBuyAccount(req.CardID), req.Amount - amount},
	)
}

// ReleaseHold returns the whole hold to open-to-buy (expiry, reversal, or a
// failed capture).
func (a *Activities) ReleaseHold(ctx context.Context, req AuthRequest) error {
	return a.Ledger.Post(ctx, stepKey(ctx, "release"),
		Entry{holdsAccount(req.CardID), -req.Amount},
		Entry{openToBuyAccount(req.CardID), req.Amount},
	)
}

// RefundCapture moves a refund from posted back to open-to-buy.
func (a *Activities) RefundCapture(ctx context.Context, req AuthRequest, r Refund) error {
	return a.Ledger.Post(ctx, stepKey(ctx, "refund:"+r.ID),
		Entry{postedAccount(req.CardID), -r.Amount},
		Entry{openToBuyAccount(req.CardID), r.Amount},
	)
}
