package main

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

const (
	CancelUpdate      = "cancel"       // stops a subscription's future charges
	SubscriptionQuery = "subscription" // the current Subscription

	DefaultSubscriptionCycle = 30 * 24 * time.Hour
)

// DefaultRetries are the waits between a declined charge's attempts: retries
// on days 1, 3 and 7, then the subscription lapses.
var DefaultRetries = []time.Duration{24 * time.Hour, 48 * time.Hour, 96 * time.Hour}

// Subscription statuses (SPEC M19). Lapsed, cancelled and ended are final.
const (
	SubActive    = "active"
	SubPastDue   = "past-due"  // the cycle's charge declined; retrying
	SubLapsed    = "lapsed"    // every retry declined
	SubCancelled = "cancelled" // the cancel update
	SubEnded     = "ended"     // the card is closed or unknown, or blocked the merchant
)

// SubscriptionWorkflowID is the ID of a subscription's workflow.
func SubscriptionWorkflowID(id string) string { return "subscription:" + id }

// SubscriptionTerms are what the cardholder signed up for. Zero values take the defaults.
type SubscriptionTerms struct {
	ID       string
	CardID   string // the number given at signup; charges go to its card's current number
	Account  string // CardID's account, resolved in the first cycle and tagged on every run
	Merchant string
	Amount   int64 // cents per cycle
	Cycle    time.Duration
	Retries  []time.Duration // waits between a declined charge's attempts
}

// Subscription is where a subscription stands, carried from cycle to cycle.
type Subscription struct {
	Cycle      int // the current cycle, from 1
	Status     string
	Charged    int64 // cents charged over every cycle
	Failures   int   // declines in a row
	LastReason string
}

// SubscriptionWorkflow runs one cycle per run: it charges at the start,
// retries a declined charge, and continues as new a full cycle after it
// began, however long the retries took, so the billing date never drifts.
func SubscriptionWorkflow(ctx workflow.Context, terms SubscriptionTerms, sub Subscription) (Subscription, error) {
	var a *Activities
	sub.Cycle++
	cancelled := false
	if err := workflow.SetQueryHandler(ctx, SubscriptionQuery, func() (Subscription, error) { return sub, nil }); err != nil {
		return sub, err
	}
	if err := workflow.SetUpdateHandler(ctx, CancelUpdate, func(workflow.Context) error { cancelled = true; return nil }); err != nil {
		return sub, err
	}
	nextCycle := workflow.NewTimer(ctx, cmp.Or(terms.Cycle, DefaultSubscriptionCycle))
	// Tagged with the card's account, so a card's subscriptions can be listed (M22).
	if workflow.GetVersion(ctx, "subscription-card-id", workflow.DefaultVersion, 1) != workflow.DefaultVersion {
		if terms.Account == "" {
			lctx := workflow.WithLocalActivityOptions(ctx, workflow.LocalActivityOptions{StartToCloseTimeout: activityTimeout,
				RetryPolicy: &temporal.RetryPolicy{}}) // resolving has nothing to compensate: retry until it answers
			var number CardNumber
			if err := workflow.ExecuteLocalActivity(lctx, a.ResolveCard, terms.CardID).Get(ctx, &number); err != nil {
				return sub, err
			}
			terms.Account = number.Account
		}
		if err := workflow.UpsertTypedSearchAttributes(ctx, CardIDAttribute.ValueSet(terms.Account)); err != nil {
			return sub, err
		}
	}
	// A charge attempt always has an answer, and a retry finds the same auth,
	// so the activity retries until it gets one.
	actx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{StartToCloseTimeout: time.Minute, RetryPolicy: &temporal.RetryPolicy{}})
	if terms.Retries == nil {
		terms.Retries = DefaultRetries
	}
	for attempt := 0; ; attempt++ {
		id := fmt.Sprintf("%s:c%d:a%d", SubscriptionWorkflowID(terms.ID), sub.Cycle, attempt)
		var dec Decision
		if err := workflow.ExecuteActivity(actx, a.ChargeCard, id, AuthRequest{CardID: terms.CardID, Amount: terms.Amount, Merchant: terms.Merchant}).Get(ctx, &dec); err != nil {
			return sub, err
		}
		if dec.Approved {
			sub.Status, sub.Charged, sub.Failures, sub.LastReason = SubActive, sub.Charged+terms.Amount, 0, ""
			break
		}
		sub.Failures, sub.LastReason = sub.Failures+1, dec.Reason
		switch {
		case slices.Contains([]string{DeclineClosed, DeclineUnknownCard, DeclineMerchantBlocked}, dec.Reason):
			sub.Status = SubEnded
			return sub, nil
		case attempt == len(terms.Retries):
			sub.Status = SubLapsed
			return sub, nil
		}
		sub.Status = SubPastDue
		if stopped, err := workflow.AwaitWithTimeout(ctx, terms.Retries[attempt], func() bool { return cancelled }); err != nil || stopped {
			sub.Status = SubCancelled
			return sub, err
		}
	}
	if err := workflow.Await(ctx, func() bool { return cancelled || nextCycle.IsReady() }); err != nil {
		return sub, err
	}
	if cancelled {
		sub.Status = SubCancelled
		return sub, nil
	}
	return sub, workflow.NewContinueAsNewError(ctx, SubscriptionWorkflow, terms, sub)
}

// ChargeCard makes one attempt at a charge as a network would: an authorize
// on the card's current number (the account updater, SPEC M19), then a full
// capture. A retry attaches to the same auth and capture (ADR 0010).
func (a *Activities) ChargeCard(ctx context.Context, authID string, req AuthRequest) (Decision, error) {
	if a.Temporal == nil { // fail the subscription where the audit sees it, not panic and retry forever
		return Decision{}, temporal.NewNonRetryableApplicationError("charges need a Temporal client", "NoClient", nil)
	}
	var err error
	if req.CardID, err = a.Config.CurrentNumber(ctx, req.CardID); err != nil {
		return Decision{}, err
	}
	dec, _, err := authorizeAndCapture(ctx, a.Temporal, authID, req)
	var started *serviceerror.WorkflowExecutionAlreadyStarted
	if errors.As(err, &started) { // a retry after the declined auth finished: its answer stands
		var res AuthResult
		if err := a.Temporal.GetWorkflow(ctx, authID, "").Get(ctx, &res); err != nil {
			return Decision{}, err
		}
		return Decision{Approved: res.Status != StatusDeclined, Reason: res.Reason}, nil
	}
	return dec, err
}
