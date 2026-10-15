package main

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
)

func dispute(at time.Duration, id string, amount int64) signal {
	return signal{at, DisputeUpdate, Dispute{ID: id, Amount: amount}}
}

func respond(at time.Duration, id string, accept bool) signal {
	return signal{at, MerchantResponseUpdate, MerchantResponse{DisputeID: id, Accept: accept}}
}

func rule(at time.Duration, id string, cardholderWins bool) signal {
	return signal{at, ResolveDisputeUpdate, Ruling{DisputeID: id, CardholderWins: cardholderWins}}
}

// Disputes run inside the auth: a provisional credit at once, then the
// merchant's response, then an analyst's ruling, with missed deadlines going
// to the cardholder. Refunds and disputes share one cap.
func TestDisputes(t *testing.T) {
	longWindow := auth("card-1", 40_000)
	longWindow.RefundWindow = 60 * day // still open after the 45-day merchant deadline
	tests := []struct {
		name  string
		req   AuthRequest // zero: auth("card-1", 40_000)
		steps []step
		want  AuthResult
		// card-1's posted balance and points afterwards
		wantPosted, wantPoints int64
	}{
		{"merchant accepts: cardholder wins", AuthRequest{},
			[]step{upd(capture(hr, 40_000), "ok"), upd(dispute(2*hr, "d1", 15_000), "ok"), upd(respond(3*hr, "d1", true), "ok")},
			AuthResult{Captured: 40_000, Status: StatusCaptured, ChargedBack: 15_000, Points: 250}, 25_000, 250},
		{"no merchant response: cardholder wins at the deadline", AuthRequest{},
			[]step{upd(capture(hr, 40_000), "ok"), upd(dispute(2*hr, "d1", 15_000), "ok")},
			AuthResult{Captured: 40_000, Status: StatusCaptured, ChargedBack: 15_000, Points: 250}, 25_000, 250},
		{"contested, merchant wins: rebilled", AuthRequest{},
			[]step{upd(capture(hr, 40_000), "ok"), upd(dispute(2*hr, "d1", 15_000), "ok"), upd(respond(3*hr, "d1", false), "ok"), upd(rule(4*hr, "d1", false), "ok")},
			AuthResult{Captured: 40_000, Status: StatusCaptured, Points: 400}, 40_000, 400},
		{"contested, cardholder wins", AuthRequest{},
			[]step{upd(capture(hr, 40_000), "ok"), upd(dispute(2*hr, "d1", 15_000), "ok"), upd(respond(3*hr, "d1", false), "ok"), upd(rule(4*hr, "d1", true), "ok")},
			AuthResult{Captured: 40_000, Status: StatusCaptured, ChargedBack: 15_000, Points: 250}, 25_000, 250},
		{"contested, no ruling: cardholder wins at the deadline", AuthRequest{},
			[]step{upd(capture(hr, 40_000), "ok"), upd(dispute(2*hr, "d1", 15_000), "ok"), upd(respond(3*hr, "d1", false), "ok")},
			AuthResult{Captured: 40_000, Status: StatusCaptured, ChargedBack: 15_000, Points: 250}, 25_000, 250},
		{"dispute over what refunds left rejected", AuthRequest{},
			[]step{upd(capture(hr, 40_000), "ok"), upd(refund(2*hr, "r1", 30_000), "ok"), upd(dispute(3*hr, "d1", 15_000), "rejected")},
			AuthResult{Captured: 40_000, Status: StatusCaptured, Refunded: 30_000, Points: 100}, 10_000, 100},
		{"refund over what disputes left rejected", AuthRequest{},
			[]step{upd(capture(hr, 40_000), "ok"), upd(dispute(2*hr, "d1", 30_000), "ok"), upd(refund(3*hr, "r1", 15_000), "rejected"), upd(respond(4*hr, "d1", true), "ok")},
			AuthResult{Captured: 40_000, Status: StatusCaptured, ChargedBack: 30_000, Points: 100}, 10_000, 100},
		{"dispute before capture rejected", AuthRequest{},
			[]step{upd(dispute(hr/2, "d1", 100), "rejected"), upd(capture(hr, 40_000), "ok")},
			AuthResult{Captured: 40_000, Status: StatusCaptured, Points: 400}, 40_000, 400},
		{"dispute ID reused rejected", AuthRequest{},
			[]step{upd(capture(hr, 40_000), "ok"), upd(dispute(2*hr, "d1", 15_000), "ok"), upd(dispute(3*hr, "d1", 5_000), "rejected"), upd(respond(4*hr, "d1", true), "ok")},
			AuthResult{Captured: 40_000, Status: StatusCaptured, ChargedBack: 15_000, Points: 250}, 25_000, 250},
		{"merchant response after the deadline rejected", longWindow,
			[]step{upd(capture(hr, 40_000), "ok"), upd(dispute(2*hr, "d1", 15_000), "ok"), upd(respond(46*day, "d1", false), "rejected")},
			AuthResult{Captured: 40_000, Status: StatusCaptured, ChargedBack: 15_000, Points: 250}, 25_000, 250},
		{"ruling before the merchant contests rejected", AuthRequest{},
			[]step{upd(capture(hr, 40_000), "ok"), upd(dispute(2*hr, "d1", 15_000), "ok"), upd(rule(3*hr, "d1", true), "rejected"), upd(respond(4*hr, "d1", true), "ok")},
			AuthResult{Captured: 40_000, Status: StatusCaptured, ChargedBack: 15_000, Points: 250}, 25_000, 250},
		{"unknown dispute rejected", AuthRequest{},
			[]step{upd(capture(hr, 40_000), "ok"), upd(respond(2*hr, "nope", true), "rejected")},
			AuthResult{Captured: 40_000, Status: StatusCaptured, Points: 400}, 40_000, 400},
		// d1 keeps the auth open past the 30-day window; d2 comes too late.
		{"dispute after the window rejected", AuthRequest{},
			[]step{upd(capture(hr, 40_000), "ok"), upd(dispute(2*hr, "d1", 15_000), "ok"), upd(dispute(31*day, "d2", 5_000), "rejected")},
			AuthResult{Captured: 40_000, Status: StatusCaptured, ChargedBack: 15_000, Points: 250}, 25_000, 250},
		{"whole charge disputed and won", AuthRequest{},
			[]step{upd(capture(hr, 40_000), "ok"), upd(dispute(2*hr, "d1", 40_000), "ok"), upd(respond(3*hr, "d1", true), "ok")},
			AuthResult{Captured: 40_000, Status: StatusRefunded, ChargedBack: 40_000}, 0, 0},
		// 2 points on $2.00: the refund claws back points(200) - points(150) = 1;
		// the lost dispute never claws, so 1 point stays, matching points(150).
		{"lost dispute between refunds keeps points exact", AuthRequest{},
			[]step{upd(capture(hr, 200), "ok"), upd(dispute(2*hr, "d1", 150), "ok"), upd(respond(3*hr, "d1", false), "ok"),
				upd(rule(4*hr, "d1", false), "ok"), upd(refund(5*hr, "r1", 50), "ok")},
			AuthResult{Captured: 200, Status: StatusCaptured, Refunded: 50, Points: 1}, 150, 1},
		// The chargeback claws points(200) - points(150) = 1; the refund then chains
		// from 150, not 200, and claws the last 1.
		{"chargeback then refund claw back every point", AuthRequest{},
			[]step{upd(capture(hr, 200), "ok"), upd(dispute(2*hr, "d1", 50), "ok"), upd(respond(3*hr, "d1", true), "ok"), upd(refund(4*hr, "r1", 150), "ok")},
			AuthResult{Captured: 200, Status: StatusRefunded, Refunded: 150, ChargedBack: 50}, 0, 0},
		// Won in the same instant: each claims its slice of the chain before posting.
		{"two chargebacks won together claw back every point", AuthRequest{},
			[]step{upd(capture(hr, 200), "ok"), upd(dispute(2*hr, "d1", 150), "ok"), upd(dispute(2*hr, "d2", 50), "ok"),
				upd(respond(3*hr, "d1", true), "ok"), upd(respond(3*hr, "d2", true), "ok")},
			AuthResult{Captured: 200, Status: StatusRefunded, ChargedBack: 200}, 0, 0},
		// Per-step rounding would leave a point: 150 claws back 2, then 50 claws 0.
		{"refund then chargeback claw back every point", AuthRequest{},
			[]step{upd(capture(hr, 200), "ok"), upd(refund(2*hr, "r1", 150), "ok"), upd(dispute(3*hr, "d1", 50), "ok"), upd(respond(4*hr, "d1", true), "ok")},
			AuthResult{Captured: 200, Status: StatusRefunded, Refunded: 150, ChargedBack: 50}, 0, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var s testsuite.WorkflowTestSuite
			env := s.NewTestWorkflowEnvironment()
			l := NewMemLedger()
			env.RegisterActivity(&Activities{Ledger: l, Config: card1()})
			got := sendSteps(env, tt.steps)
			req := tt.req
			if req.CardID == "" {
				req = auth("card-1", 40_000)
			}

			env.ExecuteWorkflow(AuthorizeWorkflow, req)

			if err := env.GetWorkflowError(); err != nil {
				t.Fatal(err)
			}
			checkSteps(t, tt.steps, got)
			var res AuthResult
			if err := env.GetWorkflowResult(&res); err != nil {
				t.Fatal(err)
			}
			if res != tt.want {
				t.Errorf("result = %+v, want %+v", res, tt.want)
			}
			if got := balance(t, l, postedAccount("card-1")); got != tt.wantPosted {
				t.Errorf("posted = %d, want %d", got, tt.wantPosted)
			}
			if got := balance(t, l, pointsAccount("card-1")); got != tt.wantPoints {
				t.Errorf("points account = %d, want %d", got, tt.wantPoints)
			}
			if got := balance(t, l, holdsAccount("card-1")); got != 0 {
				t.Errorf("holds = %d, want 0", got)
			}
			if got := l.Total(); got != 0 {
				t.Errorf("ledger Total = %d, want 0", got)
			}
		})
	}
}

func queryDisputes(t *testing.T, env *testsuite.TestWorkflowEnvironment) []DisputeState {
	t.Helper()
	v, err := env.QueryWorkflow(DisputesQuery)
	if err != nil {
		t.Fatal(err)
	}
	var ds []DisputeState
	if err := v.Get(&ds); err != nil {
		t.Fatal(err)
	}
	return ds
}

// The disputes query shows each dispute's stage as it moves.
func TestDisputesQuery(t *testing.T) {
	var s testsuite.WorkflowTestSuite
	env := s.NewTestWorkflowEnvironment()
	env.RegisterActivity(&Activities{Ledger: NewMemLedger(), Config: card1()})
	sendSteps(env, []step{upd(capture(hr, 40_000), "ok"),
		upd(dispute(2*hr, "d1", 15_000), "ok"), upd(respond(3*hr, "d1", false), "ok"), // contested
		upd(dispute(4*hr, "d2", 5_000), "ok"), upd(respond(5*hr, "d2", true), "ok")}) // accepted
	check := func(at time.Duration, want ...DisputeState) {
		env.RegisterDelayedCallback(func() {
			if got := queryDisputes(t, env); fmt.Sprint(got) != fmt.Sprint(want) {
				t.Errorf("at %v: disputes = %+v, want %+v", at, got, want)
			}
		}, at)
	}
	d1 := DisputeState{ID: "d1", Amount: 15_000}
	d2 := DisputeState{ID: "d2", Amount: 5_000}
	at := func(d DisputeState, stage string) DisputeState { d.Stage = stage; return d }
	check(2*hr+time.Minute, at(d1, StageAwaitingMerchant))
	check(3*hr+time.Minute, at(d1, StageUnderReview))
	check(5*hr+time.Minute, at(d1, StageUnderReview), at(d2, StageCardholderWon)) // accepting ends it at once
	check(40*day, at(d1, StageCardholderWon), at(d2, StageCardholderWon))         // d1's review deadline passed

	env.ExecuteWorkflow(AuthorizeWorkflow, auth("card-1", 40_000))

	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
}

// A provisional credit the ledger refuses outright fails the dispute update,
// rather than reporting a credit that never posted.
func TestDisputeCreditFailureIsReported(t *testing.T) {
	var s testsuite.WorkflowTestSuite
	env := s.NewTestWorkflowEnvironment()
	a := &Activities{Ledger: NewMemLedger(), Config: card1()}
	env.RegisterActivity(a)
	env.OnActivity(a.PostToCard, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(temporal.NewNonRetryableApplicationError("ledger refused", "LedgerRefused", nil))
	steps := []step{upd(capture(hr, 40_000), "ok"), upd(dispute(2*hr, "d1", 15_000), "error")}
	got := sendSteps(env, steps)

	env.ExecuteWorkflow(AuthorizeWorkflow, auth("card-1", 40_000))

	checkSteps(t, steps, got)
	if env.GetWorkflowError() == nil {
		t.Error("want the failed credit to fail the auth workflow")
	}
}

// The dispute update returns once the provisional credit has posted, so the
// caller can tell the cardholder it's on their account.
func TestDisputeReturnsAfterCredit(t *testing.T) {
	var s testsuite.WorkflowTestSuite
	env := s.NewTestWorkflowEnvironment()
	l := NewMemLedger()
	a := &Activities{Ledger: l, Config: card1()}
	env.RegisterActivity(a)
	env.OnActivity(a.PostToCard, mock.Anything, mock.Anything, mock.Anything, mock.Anything).After(day).Return(a.PostToCard)
	env.RegisterDelayedCallback(func() {
		env.UpdateWorkflow(CaptureSignal, "c", &testsuite.TestUpdateCallback{OnComplete: func(any, error) {}}, Capture{Amount: 40_000})
	}, hr)
	var opened time.Time
	env.RegisterDelayedCallback(func() {
		opened = env.Now()
		env.UpdateWorkflow(DisputeUpdate, "d", &testsuite.TestUpdateCallback{
			OnReject: func(err error) { t.Errorf("dispute rejected: %v", err) },
			OnComplete: func(_ any, err error) {
				if err != nil {
					t.Error(err)
				}
				if took := env.Now().Sub(opened); took < day {
					t.Errorf("dispute returned after %v, before its credit posted", took)
				}
				if got := balance(t, l, postedAccount("card-1")); got != 25_000 {
					t.Errorf("posted = %d when the dispute returned, want the credit in (25000)", got)
				}
			},
		}, Dispute{ID: "d1", Amount: 15_000})
	}, 2*hr)

	env.ExecuteWorkflow(AuthorizeWorkflow, auth("card-1", 40_000))

	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
}

// A dispute can't open while the capture's rewards are still posting: the
// reward rate isn't known yet, so a quick win would claw back at rate 0.
func TestDisputeWaitsForSettlement(t *testing.T) {
	var s testsuite.WorkflowTestSuite
	env := s.NewTestWorkflowEnvironment()
	a := &Activities{Ledger: NewMemLedger(), Config: card1()}
	env.RegisterActivity(a)
	env.OnActivity(a.AdjustRewards, mock.Anything, mock.Anything, mock.Anything).After(day).Return(a.AdjustRewards)
	steps := []step{
		sig(capture(hr, 40_000)),
		upd(dispute(hr+12*time.Hour, "d1", 15_000), "rejected"), // settlement rewards still posting
		upd(dispute(3*day, "d2", 15_000), "ok"),
		upd(respond(3*day+hr, "d2", true), "ok"),
	}
	got := sendSteps(env, steps)

	env.ExecuteWorkflow(AuthorizeWorkflow, auth("card-1", 40_000))

	checkSteps(t, steps, got)
	var res AuthResult
	if err := env.GetWorkflowResult(&res); err != nil {
		t.Fatal(err)
	}
	if res.ChargedBack != 15_000 || res.Points != 250 {
		t.Errorf("result = %+v, want 15000 charged back and 250 points", res)
	}
}

// A merchant response landing on the deadline either counts (the dispute goes
// to review) or is refused (the cardholder has already won); the update's
// answer must match what happened.
func TestMerchantResponseAtTheDeadline(t *testing.T) {
	var s testsuite.WorkflowTestSuite
	env := s.NewTestWorkflowEnvironment()
	env.RegisterActivity(&Activities{Ledger: NewMemLedger(), Config: card1()})
	req := auth("card-1", 40_000)
	req.RefundWindow = 90 * day
	deadline := 2*hr + DefaultMerchantResponse
	steps := []step{upd(capture(hr, 40_000), "ok"), upd(dispute(2*hr, "d1", 15_000), "ok")}
	got := sendSteps(env, steps)
	var outcome string
	env.RegisterDelayedCallback(func() {
		env.UpdateWorkflow(MerchantResponseUpdate, "late", &testsuite.TestUpdateCallback{
			OnReject: func(error) { outcome = "rejected" },
			OnComplete: func(_ any, err error) {
				outcome = "ok"
				if err != nil {
					outcome = "error"
				}
			},
		}, MerchantResponse{DisputeID: "d1", Accept: false})
	}, deadline)
	var stage string
	env.RegisterDelayedCallback(func() {
		stage = queryDisputes(t, env)[0].Stage
	}, deadline+hr)

	env.ExecuteWorkflow(AuthorizeWorkflow, req)

	checkSteps(t, steps, got)
	if want := map[string]string{"ok": StageUnderReview, "rejected": StageCardholderWon, "error": StageCardholderWon}[outcome]; stage != want {
		t.Errorf("response at the deadline answered %q but the dispute is %s, want %s", outcome, stage, want)
	}
}
