package main

import (
	"context"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

const (
	TaskQueue      = "card-auth"
	CaptureSignal  = "capture"
	DefaultHoldFor = 7 * 24 * time.Hour // how long an uncaptured hold lives
)

// Workflow outcomes.
const (
	StatusDeclined = "declined"
	StatusCaptured = "captured"
	StatusExpired  = "expired"
)

type AuthRequest struct {
	CardID   string
	Amount   int64 // cents
	Merchant string
	HoldFor  time.Duration // zero means DefaultHoldFor
}

// Capture is the payload of the capture signal. Amount may be less than the
// hold (partial capture); the remainder is released.
type Capture struct {
	Amount int64 // cents
}

type AuthResult struct {
	Status   string
	Captured int64 // cents posted to the card
}

// Card accounts. A hold moves money from open-to-buy into holds; a capture
// moves it from holds into posted and releases any remainder back.
func holdsAccount(card string) string     { return "card:" + card + ":holds" }
func postedAccount(card string) string    { return "card:" + card + ":posted" }
func openToBuyAccount(card string) string { return "card:" + card + ":open-to-buy" }

// AuthorizeWorkflow checks the limit, places a hold, then waits for a capture
// signal or the hold to expire, whichever comes first.
func AuthorizeWorkflow(ctx workflow.Context, req AuthRequest) (AuthResult, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{StartToCloseTimeout: 10 * time.Second})
	var a *Activities

	var ok bool
	if err := workflow.ExecuteActivity(ctx, a.CheckLimit, req).Get(ctx, &ok); err != nil {
		return AuthResult{}, err
	}
	if !ok {
		return AuthResult{Status: StatusDeclined}, nil
	}
	if err := workflow.ExecuteActivity(ctx, a.PlaceHold, req).Get(ctx, nil); err != nil {
		return AuthResult{}, err
	}

	holdFor := req.HoldFor
	if holdFor == 0 {
		holdFor = DefaultHoldFor
	}
	expiry := workflow.NewTimer(ctx, holdFor)
	captures := workflow.GetSignalChannel(ctx, CaptureSignal)

	for {
		var c Capture
		expired := false
		sel := workflow.NewSelector(ctx)
		sel.AddReceive(captures, func(ch workflow.ReceiveChannel, _ bool) { ch.Receive(ctx, &c) })
		sel.AddFuture(expiry, func(workflow.Future) { expired = true })
		sel.Select(ctx)

		if expired {
			if err := workflow.ExecuteActivity(ctx, a.ReleaseHold, req).Get(ctx, nil); err != nil {
				return AuthResult{}, err
			}
			return AuthResult{Status: StatusExpired}, nil
		}
		if c.Amount <= 0 || c.Amount > req.Amount {
			workflow.GetLogger(ctx).Warn("ignoring invalid capture", "amount", c.Amount, "hold", req.Amount)
			continue
		}
		if err := workflow.ExecuteActivity(ctx, a.CaptureHold, req, c.Amount).Get(ctx, nil); err != nil {
			return AuthResult{}, err
		}
		// Later capture signals are never read: the workflow is done, so a
		// duplicate capture can't post twice.
		return AuthResult{Status: StatusCaptured, Captured: c.Amount}, nil
	}
}

// Activities do the I/O the workflow can't: every ledger read and write.
// Each posting is keyed by "<workflow ID>:<step>", so a retried activity posts once.
type Activities struct {
	Ledger *Ledger
	Limits map[string]int64 // card ID → credit limit, cents
}

func stepKey(ctx context.Context, step string) string {
	return activity.GetInfo(ctx).WorkflowExecution.ID + ":" + step
}

func (a *Activities) CheckLimit(_ context.Context, req AuthRequest) (bool, error) {
	if req.Amount <= 0 {
		return false, temporal.NewNonRetryableApplicationError("amount must be positive", "InvalidAmount", nil)
	}
	limit, known := a.Limits[req.CardID]
	if !known {
		return false, nil
	}
	used := a.Ledger.Balance(holdsAccount(req.CardID)) + a.Ledger.Balance(postedAccount(req.CardID))
	// ponytail: check-then-hold races between concurrent auths on one card;
	// M3 moves the limit check into the same MySQL transaction as the hold.
	return used+req.Amount <= limit, nil
}

// PlaceHold moves the amount from open-to-buy into holds.
func (a *Activities) PlaceHold(ctx context.Context, req AuthRequest) error {
	return a.Ledger.Post(stepKey(ctx, "hold"),
		Entry{holdsAccount(req.CardID), req.Amount},
		Entry{openToBuyAccount(req.CardID), -req.Amount},
	)
}

// CaptureHold clears the hold, posts the captured amount, and releases the rest.
func (a *Activities) CaptureHold(ctx context.Context, req AuthRequest, amount int64) error {
	return a.Ledger.Post(stepKey(ctx, "capture"),
		Entry{holdsAccount(req.CardID), -req.Amount},
		Entry{postedAccount(req.CardID), amount},
		Entry{openToBuyAccount(req.CardID), req.Amount - amount},
	)
}

// ReleaseHold returns an expired hold to open-to-buy.
func (a *Activities) ReleaseHold(ctx context.Context, req AuthRequest) error {
	return a.Ledger.Post(stepKey(ctx, "release"),
		Entry{holdsAccount(req.CardID), -req.Amount},
		Entry{openToBuyAccount(req.CardID), req.Amount},
	)
}
