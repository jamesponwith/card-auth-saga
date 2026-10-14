package main

import (
	"cmp"
	"context"
	"errors"
	"fmt"
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
	AuthorizeUpdate     = "authorize"         // returns the approve/decline decision; send with Update-with-Start
	DefaultHoldFor      = 7 * 24 * time.Hour  // how long an uncaptured hold lives
	DefaultRefundWindow = 30 * 24 * time.Hour // how long after capture refunds are accepted
	DefaultRewardRate   = 1                   // points per $1 when the merchant has no partner rate
)

// activityTimeout bounds one activity attempt. A var so the timeout drill can shrink it.
var activityTimeout = 10 * time.Second

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
	Points   int64 // rewards points the card keeps after any clawback
}

// Card accounts. A hold moves money from open-to-buy into holds; a capture
// moves it from holds into posted and releases any remainder; a refund moves
// it from posted back to open-to-buy.
func holdsAccount(card string) string     { return "card:" + card + ":holds" }
func postedAccount(card string) string    { return "card:" + card + ":posted" }
func openToBuyAccount(card string) string { return "card:" + card + ":open-to-buy" }

// Rewards accounts, in points. The brand partner funds the points it issues.
func pointsAccount(card string) string            { return "card:" + card + ":points" }
func partnerPointsAccount(merchant string) string { return "partner:" + merchant + ":points-issued" }

// pointsFor is the reward for an amount at a rate (points per $1), rounded down.
func pointsFor(cents, rate int64) int64 { return cents * rate / 100 }

// AuthorizeWorkflow places a hold if the card's limit allows it, then waits for
// a capture, a reversal, or expiry. After a capture it accepts refunds until
// the refund window closes or the capture is fully refunded.
//
// Capture, reverse and refund arrive as signals or as updates (same names).
// Both routes run the same steps; an update's validator rejects a bad request
// before it reaches history, and the update returns once the ledger posts.
func AuthorizeWorkflow(ctx workflow.Context, req AuthRequest) (AuthResult, error) {
	ctx = withLedger(ctx)
	s := &authState{req: req, phase: phaseDeciding, seen: map[string]bool{}}
	if err := s.registerUpdates(ctx); err != nil {
		return AuthResult{}, err
	}

	// Waiting authorize updates run in the same workflow task that ends
	// deciding, so they answer before a decline or error closes the workflow.
	var held bool
	s.holdErr = workflow.ExecuteActivity(ctx, s.a.PlaceHold, req).Get(ctx, &held)
	switch {
	case s.holdErr != nil:
		s.phase = phaseFailed
		return AuthResult{}, s.holdErr
	case !held:
		s.phase = StatusDeclined
		return AuthResult{Status: StatusDeclined}, nil
	}
	s.phase = phaseHolding

	expiry := workflow.NewTimer(ctx, cmp.Or(req.HoldFor, DefaultHoldFor))
	workflow.Go(ctx, func(ctx workflow.Context) { _ = expiry.Get(ctx, nil); _ = s.release(ctx, StatusExpired) })
	receive(ctx, ReverseSignal, func(ctx workflow.Context, _ *struct{}) error { return s.release(ctx, StatusReversed) })
	receive(ctx, CaptureSignal, func(ctx workflow.Context, c *Capture) error { return s.capture(ctx, *c) })
	if err := workflow.Await(ctx, func() bool { return s.phase != phaseHolding && s.phase != phaseBusy }); err != nil {
		return AuthResult{}, err
	}
	if s.err != nil || s.phase != StatusCaptured {
		return AuthResult{Status: s.phase}, s.err
	}

	// Rewards never undo money movement, so their activities retry until the
	// ledger takes them. The rate is fixed at settlement and reused for refunds.
	rctx := workflow.WithRetryPolicy(ctx, temporal.RetryPolicy{})
	if err := workflow.ExecuteActivity(rctx, s.a.RewardRate, req.Merchant).Get(ctx, &s.rate); err != nil {
		return AuthResult{}, err
	}
	if err := workflow.ExecuteActivity(rctx, s.a.AdjustRewards, req, PointsChange{"rewards", s.rate, 0, s.captured}).Get(ctx, nil); err != nil {
		return AuthResult{}, err
	}

	window := workflow.NewTimer(ctx, cmp.Or(req.RefundWindow, DefaultRefundWindow))
	s.refundsOpen = true
	workflow.Go(ctx, func(ctx workflow.Context) { _ = window.Get(ctx, nil); s.refundsClosed = true })
	receive(ctx, RefundSignal, func(ctx workflow.Context, r *Refund) error { return s.refund(ctx, *r) })
	// In-flight refunds finish even if the window closes under them.
	if err := workflow.Await(ctx, func() bool {
		return s.err != nil || s.reserved == s.refunded && (s.refundsClosed || s.refunded == s.captured)
	}); err != nil {
		return AuthResult{}, err
	}
	if s.err != nil {
		return AuthResult{}, s.err
	}
	res := AuthResult{Status: StatusCaptured, Captured: s.captured, Refunded: s.refunded, Points: pointsFor(s.captured-s.refunded, s.rate)}
	if s.refunded == s.captured {
		res.Status = StatusRefunded
	}
	return res, nil
}

// withLedger sets the options for ledger activities. Update handlers get a
// fresh context, so each step applies these itself.
func withLedger(ctx workflow.Context) workflow.Context {
	return workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: activityTimeout,
		// Bounded so a ledger that keeps failing surfaces here, where the saga
		// can compensate, instead of retrying forever.
		RetryPolicy: &temporal.RetryPolicy{MaximumAttempts: 5},
	})
}

// receive runs f for each signal named name, one at a time. A signal f
// rejects is logged and dropped: unlike an update, nobody is waiting on it.
func receive[T any](ctx workflow.Context, name string, f func(workflow.Context, *T) error) {
	ch := workflow.GetSignalChannel(ctx, name)
	workflow.Go(ctx, func(ctx workflow.Context) {
		for {
			var v T
			ch.Receive(ctx, &v)
			if err := f(ctx, &v); err != nil {
				workflow.GetLogger(ctx).Warn("signal not applied", "signal", name, "payload", v, "error", err)
			}
		}
	})
}

// Hold phases. After the hold, phase ends as one of the Status values.
const (
	phaseDeciding = "deciding" // PlaceHold running
	phaseHolding  = "holding"  // waiting for capture, reversal or expiry
	phaseBusy     = "busy"     // a capture or release has claimed the hold
	phaseFailed   = "failed"   // a capture or release could not post; see err
)

// authState is one authorization, shared by the main workflow, the signal
// loops and the update handlers. Workflow coroutines take turns and never run
// in parallel, so a check and the state change after it can't interleave:
// each step claims its share (phase, seen, reserved) before its first activity.
type authState struct {
	a       *Activities
	req     AuthRequest
	phase   string
	holdErr error // PlaceHold's error, for the authorize update
	err     error // a capture, release or refund that could not post

	captured, rate int64
	refundsOpen    bool            // settlement rewards posted
	refundsClosed  bool            // window closed or workflow finishing
	seen           map[string]bool // refund IDs claimed
	reserved       int64           // refunded plus refunds still posting
	refunded       int64
}

func (s *authState) registerUpdates(ctx workflow.Context) error {
	return errors.Join(
		workflow.SetUpdateHandler(ctx, AuthorizeUpdate, func(ctx workflow.Context) (bool, error) {
			err := workflow.Await(ctx, func() bool { return s.phase != phaseDeciding })
			return s.phase != StatusDeclined, cmp.Or(err, s.holdErr)
		}),
		workflow.SetUpdateHandlerWithOptions(ctx, CaptureSignal, s.capture,
			workflow.UpdateHandlerOptions{Validator: func(_ workflow.Context, c Capture) error { return s.checkCapture(c) }}),
		workflow.SetUpdateHandlerWithOptions(ctx, ReverseSignal, func(ctx workflow.Context) error { return s.release(ctx, StatusReversed) },
			workflow.UpdateHandlerOptions{Validator: func(workflow.Context) error { return s.checkHeld() }}),
		workflow.SetUpdateHandlerWithOptions(ctx, RefundSignal, s.refund,
			workflow.UpdateHandlerOptions{Validator: func(_ workflow.Context, r Refund) error { return s.checkRefund(r) }}),
	)
}

func (s *authState) checkHeld() error {
	if s.phase != phaseHolding {
		return fmt.Errorf("hold is %s", s.phase)
	}
	return nil
}

func (s *authState) checkCapture(c Capture) error {
	if c.Amount <= 0 || c.Amount > s.req.Amount {
		return fmt.Errorf("capture %d outside the %d hold", c.Amount, s.req.Amount)
	}
	return s.checkHeld()
}

// capture posts c and releases the rest of the hold.
func (s *authState) capture(ctx workflow.Context, c Capture) error {
	if err := s.checkCapture(c); err != nil {
		return err
	}
	ctx = withLedger(ctx)
	s.phase = phaseBusy
	if err := workflow.ExecuteActivity(ctx, s.a.CaptureHold, s.req, c.Amount).Get(ctx, nil); err != nil {
		// Compensate: a capture that can't post must not leave the hold
		// eating the card's limit.
		if rerr := workflow.ExecuteActivity(ctx, s.a.ReleaseHold, s.req).Get(ctx, nil); rerr != nil {
			workflow.GetLogger(ctx).Error("compensating release failed", "error", rerr)
		}
		s.phase, s.err = phaseFailed, err
		return err
	}
	s.phase, s.captured = StatusCaptured, c.Amount
	// Return once settlement rewards post, so a caller can refund right away.
	return workflow.Await(ctx, func() bool { return s.refundsOpen })
}

// release returns the whole hold to open-to-buy and ends as status.
func (s *authState) release(ctx workflow.Context, status string) error {
	if err := s.checkHeld(); err != nil {
		return err
	}
	ctx = withLedger(ctx)
	s.phase = phaseBusy
	if err := workflow.ExecuteActivity(ctx, s.a.ReleaseHold, s.req).Get(ctx, nil); err != nil {
		s.phase, s.err = phaseFailed, err
		return err
	}
	s.phase = status
	return nil
}

func (s *authState) checkRefund(r Refund) error {
	switch {
	case !s.refundsOpen:
		return fmt.Errorf("refunds not open: hold is %s, or settlement rewards still posting", s.phase)
	case s.refundsClosed:
		return errors.New("refund window closed")
	case r.ID == "" || s.seen[r.ID]:
		return fmt.Errorf("refund ID %q empty or already used", r.ID)
	case r.Amount <= 0 || s.reserved+r.Amount > s.captured:
		return fmt.Errorf("refund %d outside the %d left", r.Amount, s.captured-s.reserved)
	}
	return nil
}

// refund posts r and claws back its rewards.
func (s *authState) refund(ctx workflow.Context, r Refund) error {
	if err := s.checkRefund(r); err != nil {
		return err
	}
	ctx = withLedger(ctx)
	s.seen[r.ID] = true
	left := s.captured - s.reserved
	s.reserved += r.Amount
	if err := workflow.ExecuteActivity(ctx, s.a.RefundCapture, s.req, r).Get(ctx, nil); err != nil {
		s.err = err
		return err
	}
	// Claw back by what the remaining spend is now worth, not by the
	// refund's own (rounded) points, so a full refund lands on exactly 0.
	rctx := workflow.WithRetryPolicy(ctx, temporal.RetryPolicy{})
	if err := workflow.ExecuteActivity(rctx, s.a.AdjustRewards, s.req, PointsChange{"rewards:refund:" + r.ID, s.rate, left, left - r.Amount}).Get(ctx, nil); err != nil {
		s.err = err
		return err
	}
	s.refunded += r.Amount
	return nil
}

// Activities do the I/O the workflow can't: every ledger read and write.
// Each posting is keyed by "<workflow ID>:<step>", so a retried activity posts once.
type Activities struct {
	Ledger Ledger
	Limits map[string]int64 // card ID → credit limit, cents
	Rates  map[string]int64 // merchant → partner reward rate, points per $1
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

// RewardRate is the merchant's partner rate, read once per settlement so the
// rate is recorded in workflow history and reused for every clawback.
func (a *Activities) RewardRate(_ context.Context, merchant string) (int64, error) {
	return cmp.Or(a.Rates[merchant], DefaultRewardRate), nil
}

// PointsChange moves a card's points from what Before cents earned to what
// After cents earn at Rate. Step names the idempotency key.
type PointsChange struct {
	Step          string
	Rate          int64
	Before, After int64 // cents
}

// AdjustRewards posts the points difference between the partner and the card.
func (a *Activities) AdjustRewards(ctx context.Context, req AuthRequest, c PointsChange) error {
	points := pointsFor(c.After, c.Rate) - pointsFor(c.Before, c.Rate)
	if points == 0 {
		return nil
	}
	return a.Ledger.Post(ctx, stepKey(ctx, c.Step),
		Entry{pointsAccount(req.CardID), points},
		Entry{partnerPointsAccount(req.Merchant), -points},
	)
}
