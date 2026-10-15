package main

import (
	"cmp"
	"errors"
	"fmt"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

const (
	DisputeUpdate          = "dispute"           // open a dispute; see Dispute
	MerchantResponseUpdate = "merchant-response" // the merchant accepts or contests; see MerchantResponse
	ResolveDisputeUpdate   = "resolve-dispute"   // the analyst's ruling on a contested dispute; see Ruling
	DisputesQuery          = "disputes"          // []DisputeState

	DefaultMerchantResponse = 45 * 24 * time.Hour // how long the merchant has to accept or contest
	DefaultDisputeReview    = 30 * 24 * time.Hour // how long the analyst has to rule on a contest
)

// Dispute stages. A dispute ends cardholder-won or merchant-won.
const (
	StageAwaitingMerchant = "awaiting-merchant"
	StageUnderReview      = "under-review"
	StageCardholderWon    = "cardholder-won"
	StageMerchantWon      = "merchant-won"
)

// Dispute opens a dispute on part or all of a captured charge.
type Dispute struct {
	ID     string
	Amount int64 // cents
}

// MerchantResponse is the merchant's answer, relayed by the network.
type MerchantResponse struct {
	DisputeID string
	Accept    bool // false contests the dispute, sending it to review
}

// Ruling is the analyst's decision on a contested dispute.
type Ruling struct {
	DisputeID      string
	CardholderWins bool
}

// DisputeState is one dispute as the disputes query shows it.
type DisputeState struct {
	ID     string
	Amount int64
	Stage  string
}

type disputeState struct {
	DisputeState
	credited bool // provisional credit posted
}

func (s *authState) registerDisputes(ctx workflow.Context) error {
	return errors.Join(
		workflow.SetUpdateHandlerWithOptions(ctx, DisputeUpdate, s.openDispute,
			workflow.UpdateHandlerOptions{Validator: func(_ workflow.Context, d Dispute) error { return s.checkDispute(d) }}),
		workflow.SetUpdateHandlerWithOptions(ctx, MerchantResponseUpdate, s.merchantResponds,
			workflow.UpdateHandlerOptions{Validator: func(_ workflow.Context, r MerchantResponse) error {
				_, err := s.disputeAt(r.DisputeID, StageAwaitingMerchant)
				return err
			}}),
		workflow.SetUpdateHandlerWithOptions(ctx, ResolveDisputeUpdate, s.analystRules,
			workflow.UpdateHandlerOptions{Validator: func(_ workflow.Context, r Ruling) error {
				_, err := s.disputeAt(r.DisputeID, StageUnderReview)
				return err
			}}),
		workflow.SetQueryHandler(ctx, DisputesQuery, func() ([]DisputeState, error) {
			out := make([]DisputeState, len(s.disputeOrder))
			for i, id := range s.disputeOrder {
				out[i] = s.disputes[id].DisputeState
			}
			return out, nil
		}),
	)
}

func (s *authState) checkDispute(d Dispute) error {
	switch {
	case !s.refundsOpen:
		return fmt.Errorf("disputes open once the charge settles: hold is %s", s.phase)
	case s.refundsClosed:
		return errors.New("dispute window closed")
	case d.ID == "" || s.disputes[d.ID] != nil:
		return fmt.Errorf("dispute ID %q empty or already used", d.ID)
	case d.Amount <= 0 || s.reserved+s.disputed+d.Amount > s.captured:
		return fmt.Errorf("dispute %d outside the %d left", d.Amount, s.captured-s.reserved-s.disputed)
	}
	return nil
}

// disputeAt is the dispute with this ID, if it's at the given stage.
func (s *authState) disputeAt(id, stage string) (*disputeState, error) {
	ds := s.disputes[id]
	switch {
	case ds == nil:
		return nil, fmt.Errorf("no dispute %q", id)
	case ds.Stage != stage:
		return nil, fmt.Errorf("dispute %q is %s", id, ds.Stage)
	}
	return ds, nil
}

// openDispute claims the amount, credits the card provisionally, and leaves
// the rest of the dispute to run on its own. It returns once the credit posts.
func (s *authState) openDispute(ctx workflow.Context, d Dispute) error {
	if err := s.checkDispute(d); err != nil {
		return err
	}
	ds := &disputeState{DisputeState: DisputeState{ID: d.ID, Amount: d.Amount, Stage: StageAwaitingMerchant}}
	s.disputes[d.ID] = ds
	s.disputeOrder = append(s.disputeOrder, d.ID)
	s.disputed += d.Amount
	s.openDisputes++
	workflow.Go(s.root, func(ctx workflow.Context) {
		defer func() { s.openDisputes-- }()
		if err := s.runDispute(ctx, ds); err != nil {
			s.err = err
		}
	})
	if err := workflow.Await(ctx, func() bool { return ds.credited || s.err != nil }); err != nil || ds.credited {
		return err
	}
	return s.err
}

// runDispute posts the provisional credit, then waits out the merchant and,
// if contested, the analyst. A missed deadline goes to the cardholder.
func (s *authState) runDispute(ctx workflow.Context, ds *disputeState) error {
	// A credit or rebill has nothing to compensate with: retry until the ledger answers.
	ctx = workflow.WithRetryPolicy(withLedger(ctx), temporal.RetryPolicy{})
	if err := workflow.ExecuteActivity(ctx, s.a.PostToCard, s.req.CardID, "dispute:"+ds.ID+":credit", -ds.Amount).Get(ctx, nil); err != nil {
		return err
	}
	ds.credited = true

	// The handlers move the stage on; a deadline that passes first decides
	// for the cardholder.
	for _, wait := range []struct {
		stage   string
		timeout time.Duration
	}{
		{StageAwaitingMerchant, cmp.Or(s.req.MerchantResponse, DefaultMerchantResponse)},
		{StageUnderReview, cmp.Or(s.req.DisputeReview, DefaultDisputeReview)},
	} {
		if ds.Stage != wait.stage {
			continue
		}
		moved, err := workflow.AwaitWithTimeout(ctx, wait.timeout, func() bool { return ds.Stage != wait.stage })
		if err != nil {
			return err
		}
		if !moved {
			ds.Stage = StageCardholderWon
		}
	}

	if ds.Stage == StageMerchantWon {
		return workflow.ExecuteActivity(ctx, s.a.PostToCard, s.req.CardID, "dispute:"+ds.ID+":rebill", ds.Amount).Get(ctx, nil)
	}
	// The credit stands; claw back the dispute's points from the remaining
	// spend, claimed now so clawbacks always chain to exact points.
	left := s.captured - s.clawed
	s.clawed += ds.Amount
	if err := workflow.ExecuteActivity(ctx, s.a.AdjustRewards, s.req, PointsChange{"rewards:dispute:" + ds.ID, s.rate, left, left - ds.Amount}).Get(ctx, nil); err != nil {
		return err
	}
	s.chargedBack += ds.Amount
	return nil
}

// merchantResponds records the merchant's answer: accepting ends the dispute
// for the cardholder, contesting sends it to review. The deadline may have
// passed since the validator ran, so it checks the stage again.
func (s *authState) merchantResponds(_ workflow.Context, r MerchantResponse) error {
	ds, err := s.disputeAt(r.DisputeID, StageAwaitingMerchant)
	if err != nil {
		return err
	}
	ds.Stage = StageUnderReview
	if r.Accept {
		ds.Stage = StageCardholderWon
	}
	return nil
}

// analystRules records the ruling on a contested dispute, checking the stage
// again in case the review deadline has passed.
func (s *authState) analystRules(_ workflow.Context, r Ruling) error {
	ds, err := s.disputeAt(r.DisputeID, StageUnderReview)
	if err != nil {
		return err
	}
	ds.Stage = StageMerchantWon
	if r.CardholderWins {
		ds.Stage = StageCardholderWon
	}
	return nil
}
