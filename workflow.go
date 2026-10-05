package main

import (
	"context"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

const TaskQueue = "card-auth"

type AuthRequest struct {
	CardID   string
	Amount   int64 // cents
	Merchant string
}

type AuthResult struct {
	Approved bool
	Reason   string // why it was declined
}

func holdsAccount(card string) string     { return "card:" + card + ":holds" }
func openToBuyAccount(card string) string { return "card:" + card + ":open-to-buy" }

// AuthorizeWorkflow checks the card's limit and places a hold. Capture and
// expiry (signals + timers) arrive in M2.
func AuthorizeWorkflow(ctx workflow.Context, req AuthRequest) (AuthResult, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{StartToCloseTimeout: 10 * time.Second})
	var a *Activities

	var ok bool
	if err := workflow.ExecuteActivity(ctx, a.CheckLimit, req).Get(ctx, &ok); err != nil {
		return AuthResult{}, err
	}
	if !ok {
		return AuthResult{Reason: "insufficient open-to-buy"}, nil
	}

	if err := workflow.ExecuteActivity(ctx, a.PlaceHold, req).Get(ctx, nil); err != nil {
		return AuthResult{}, err
	}
	return AuthResult{Approved: true}, nil
}

// Activities do the I/O the workflow can't: every ledger read and write.
type Activities struct {
	Ledger *Ledger
	Limits map[string]int64 // card ID → credit limit, cents
}

func (a *Activities) CheckLimit(_ context.Context, req AuthRequest) (bool, error) {
	if req.Amount <= 0 {
		return false, temporal.NewNonRetryableApplicationError("amount must be positive", "InvalidAmount", nil)
	}
	limit, known := a.Limits[req.CardID]
	if !known {
		return false, nil
	}
	// ponytail: check-then-hold races between concurrent auths on one card;
	// M3 moves the limit check into the same MySQL transaction as the hold.
	return a.Ledger.Balance(holdsAccount(req.CardID))+req.Amount <= limit, nil
}

// PlaceHold moves the amount from open-to-buy into holds, keyed by
// "<workflow ID>:hold" so a retried activity posts once.
func (a *Activities) PlaceHold(ctx context.Context, req AuthRequest) error {
	return a.Ledger.Post(activity.GetInfo(ctx).WorkflowExecution.ID+":hold",
		Entry{holdsAccount(req.CardID), req.Amount},
		Entry{openToBuyAccount(req.CardID), -req.Amount},
	)
}
