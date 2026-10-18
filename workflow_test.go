package main

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
)

// card1 is the config most tests run with: card-1 has a $1,000 limit.
func card1() StaticConfig { return StaticConfig{Limits: map[string]int64{"card-1": 100_000}} }

func auth(card string, amount int64) AuthRequest {
	return AuthRequest{CardID: card, Amount: amount, Merchant: "shop"}
}

// signal is sent at `at` of workflow time.
type signal struct {
	at      time.Duration
	name    string
	payload any
}

func capture(at time.Duration, amount int64) signal {
	return signal{at, CaptureSignal, Capture{Amount: amount}}
}

func refund(at time.Duration, id string, amount int64) signal {
	return signal{at, RefundSignal, Refund{ID: id, Amount: amount}}
}

func reverse(at time.Duration) signal { return signal{at, ReverseSignal, nil} }

func partial(at time.Duration, amount int64) signal {
	return signal{at, CaptureSignal, Capture{Amount: amount, Partial: true}}
}

func increment(at time.Duration, id string, amount int64) signal {
	return signal{at, IncrementUpdate, Increment{ID: id, Amount: amount}}
}

const hr = time.Hour

func TestAuthorizeWorkflow(t *testing.T) {
	shortHold := auth("card-1", 40_000)
	shortHold.HoldFor = 30 * time.Minute // expires before a 1h capture
	shortRefunds := auth("card-1", 40_000)
	shortRefunds.RefundWindow = hr // capture at 1h, window closes at 2h

	tests := []struct {
		name         string
		seedHolds    int64 // card-1 balances before this auth
		seedPosted   int64
		req          AuthRequest
		signals      []signal
		want         string // a Status, or "error"
		wantRefunded int64
		wantHolds    int64 // card-1 balances afterwards
		wantPosted   int64
		wantPoints   int64 // shop earns 1 point per $1
	}{
		{"full capture", 0, 0, auth("card-1", 40_000), []signal{capture(hr, 40_000)}, StatusCaptured, 0, 0, 40_000, 400},
		{"partial capture releases the rest", 0, 0, auth("card-1", 40_000), []signal{capture(hr, 25_000)}, StatusCaptured, 0, 0, 25_000, 250},
		{"no capture expires", 0, 0, auth("card-1", 40_000), nil, StatusExpired, 0, 0, 0, 0},
		{"hold expires before capture", 0, 0, shortHold, []signal{capture(hr, 40_000)}, StatusExpired, 0, 0, 0, 0},
		{"invalid captures ignored", 0, 0, auth("card-1", 40_000), []signal{capture(hr, 50_000), capture(hr, 0), capture(hr, 30_000)}, StatusCaptured, 0, 0, 30_000, 300},
		{"duplicate capture posts once", 0, 0, auth("card-1", 40_000), []signal{capture(hr, 40_000), capture(hr, 40_000)}, StatusCaptured, 0, 0, 40_000, 400},
		{"reversal releases the hold", 0, 0, auth("card-1", 40_000), []signal{reverse(hr)}, StatusReversed, 0, 0, 0, 0},
		{"capture after reversal ignored", 0, 0, auth("card-1", 40_000), []signal{reverse(hr), capture(2*hr, 40_000)}, StatusReversed, 0, 0, 0, 0},
		{"partial refund", 0, 0, auth("card-1", 40_000), []signal{capture(hr, 40_000), refund(2*hr, "r1", 15_000)}, StatusCaptured, 15_000, 0, 25_000, 250},
		{"refunded in two parts", 0, 0, auth("card-1", 40_000), []signal{capture(hr, 40_000), refund(2*hr, "r1", 15_000), refund(3*hr, "r2", 25_000)}, StatusRefunded, 40_000, 0, 0, 0},
		{"resent refund ID posts once", 0, 0, auth("card-1", 40_000), []signal{capture(hr, 40_000), refund(2*hr, "r1", 15_000), refund(3*hr, "r1", 15_000)}, StatusCaptured, 15_000, 0, 25_000, 250},
		{"over-refund ignored", 0, 0, auth("card-1", 40_000), []signal{capture(hr, 25_000), refund(2*hr, "r1", 30_000)}, StatusCaptured, 0, 0, 25_000, 250},
		{"refund after window ignored", 0, 0, shortRefunds, []signal{capture(hr, 40_000), refund(3*hr, "r1", 10_000)}, StatusCaptured, 0, 0, 40_000, 400},
		{"exactly the limit", 0, 0, auth("card-1", 100_000), []signal{capture(hr, 100_000)}, StatusCaptured, 0, 0, 100_000, 1000},
		{"over limit", 0, 0, auth("card-1", 100_001), nil, StatusDeclined, 0, 0, 0, 0},
		{"over limit after earlier hold", 70_000, 0, auth("card-1", 40_000), nil, StatusDeclined, 0, 70_000, 0, 0},
		{"over limit after earlier spend", 0, 70_000, auth("card-1", 40_000), nil, StatusDeclined, 0, 0, 70_000, 0},
		{"unknown card", 0, 0, auth("card-x", 100), nil, StatusDeclined, 0, 0, 0, 0},
		{"non-positive amount", 0, 0, auth("card-1", 0), nil, "error", 0, 0, 0, 0},
		// Per-refund rounding would claw back 1 + 0 of the 2 points earned.
		{"odd refunds claw back every point", 0, 0, auth("card-1", 200), []signal{capture(hr, 200), refund(2*hr, "r1", 150), refund(3*hr, "r2", 50)}, StatusRefunded, 200, 0, 0, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var s testsuite.WorkflowTestSuite
			env := s.NewTestWorkflowEnvironment()
			l := NewMemLedger()
			if err := l.Post(t.Context(), "seed",
				Entry{holdsAccount("card-1"), tt.seedHolds},
				Entry{postedAccount("card-1"), tt.seedPosted},
				Entry{openToBuyAccount("card-1"), -tt.seedHolds - tt.seedPosted},
			); err != nil {
				t.Fatal(err)
			}
			env.RegisterActivity(&Activities{Ledger: l, Config: card1()})
			for _, sig := range tt.signals {
				env.RegisterDelayedCallback(func() { env.SignalWorkflow(sig.name, sig.payload) }, sig.at)
			}

			env.ExecuteWorkflow(AuthorizeWorkflow, tt.req)

			if !env.IsWorkflowCompleted() {
				t.Fatal("workflow did not complete")
			}
			got := "error"
			var res AuthResult
			if env.GetWorkflowError() == nil {
				if err := env.GetWorkflowResult(&res); err != nil {
					t.Fatal(err)
				}
				got = res.Status
			}
			if got != tt.want {
				t.Errorf("outcome = %s, want %s (err %v)", got, tt.want, env.GetWorkflowError())
			}
			if res.Refunded != tt.wantRefunded {
				t.Errorf("Refunded = %d, want %d", res.Refunded, tt.wantRefunded)
			}
			if got := balance(t, l, holdsAccount("card-1")); got != tt.wantHolds {
				t.Errorf("holds = %d, want %d", got, tt.wantHolds)
			}
			if got := balance(t, l, postedAccount("card-1")); got != tt.wantPosted {
				t.Errorf("posted = %d, want %d", got, tt.wantPosted)
			}
			if res.Points != tt.wantPoints {
				t.Errorf("Points = %d, want %d", res.Points, tt.wantPoints)
			}
			if got := balance(t, l, pointsAccount("card-1")); got != tt.wantPoints {
				t.Errorf("points account = %d, want %d", got, tt.wantPoints)
			}
			if got := l.Total(); got != 0 {
				t.Errorf("ledger Total = %d, want 0", got)
			}
		})
	}
}

// The authorize update answers as soon as the hold is decided, while the
// workflow keeps running; a late or repeated update gets the same answer.
func TestAuthorizeUpdate(t *testing.T) {
	tests := []struct {
		name    string
		req     AuthRequest
		at      time.Duration // when the update arrives
		want    Decision
		wantErr bool
	}{
		{"approved", auth("card-1", 40_000), 0, Decision{Approved: true}, false},
		{"over limit declined", auth("card-1", 100_001), 0, Decision{Reason: DeclineOverLimit}, false},
		{"unknown card declined", auth("card-x", 100), 0, Decision{Reason: DeclineUnknownCard}, false},
		{"non-positive amount errors", auth("card-1", 0), 0, Decision{}, true},
		{"late update after capture", auth("card-1", 40_000), 2 * hr, Decision{Approved: true}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var s testsuite.WorkflowTestSuite
			env := s.NewTestWorkflowEnvironment()
			env.RegisterActivity(&Activities{Ledger: NewMemLedger(), Config: card1()})
			env.RegisterDelayedCallback(func() { env.SignalWorkflow(CaptureSignal, Capture{Amount: 40_000}) }, hr)
			var calls int
			var got any
			var gotErr error
			for _, id := range []string{"u1", "u2"} { // a retried request is a second update
				env.RegisterDelayedCallback(func() {
					env.UpdateWorkflow(AuthorizeUpdate, id, &testsuite.TestUpdateCallback{
						OnReject:   func(err error) { t.Errorf("update rejected: %v", err) },
						OnComplete: func(r any, err error) { calls++; got, gotErr = r, err },
					})
				}, tt.at)
			}

			env.ExecuteWorkflow(AuthorizeWorkflow, tt.req)

			if calls != 2 {
				t.Fatalf("update completed %d times, want 2", calls)
			}
			if (gotErr != nil) != tt.wantErr {
				t.Fatalf("update err = %v, wantErr %v", gotErr, tt.wantErr)
			}
			if !tt.wantErr && got != tt.want {
				t.Errorf("decision = %v, want %v", got, tt.want)
			}
		})
	}
}

// step is a signal or an update sent at `at`; want is the update's outcome
// ("ok", "declined" when it returns false, "rejected" by its validator, or
// "error"), empty for a signal.
type step struct {
	signal
	want string
}

func upd(s signal, want string) step { return step{s, want} }
func sig(s signal) step              { return step{s, ""} }

// sendSteps schedules each step and returns the update outcomes, which fill in
// as the workflow runs.
func sendSteps(env *testsuite.TestWorkflowEnvironment, steps []step) []string {
	got := make([]string, len(steps))
	for i, st := range steps {
		env.RegisterDelayedCallback(func() {
			if st.want == "" {
				env.SignalWorkflow(st.name, st.payload)
				return
			}
			var args []any
			if st.payload != nil {
				args = append(args, st.payload)
			}
			env.UpdateWorkflow(st.name, fmt.Sprint(i), &testsuite.TestUpdateCallback{
				OnReject: func(error) { got[i] = "rejected" },
				OnComplete: func(r any, err error) {
					switch {
					case err != nil:
						got[i] = "error"
					case r == false:
						got[i] = "declined"
					default:
						got[i] = "ok"
					}
				},
			}, args...)
		}, st.at)
	}
	return got
}

func checkSteps(t *testing.T, steps []step, got []string) {
	t.Helper()
	for i, st := range steps {
		if got[i] != st.want {
			t.Errorf("step %d (%s): %q, want %q", i, st.name, got[i], st.want)
		}
	}
}

// Updates tell the caller what happened to the money; validators reject bad
// requests before they reach history, and concurrent requests can't overspend.
func TestMoneyUpdates(t *testing.T) {
	tests := []struct {
		name         string
		steps        []step
		want         string
		wantRefunded int64
		wantPosted   int64
		wantPoints   int64
	}{
		{"capture", []step{upd(capture(hr, 25_000), "ok")}, StatusCaptured, 0, 25_000, 250},
		{"bad capture amount rejected", []step{upd(capture(hr, 50_000), "rejected"), upd(capture(hr, 0), "rejected"), upd(capture(hr, 30_000), "ok")}, StatusCaptured, 0, 30_000, 300},
		{"second capture rejected", []step{upd(capture(hr, 40_000), "ok"), upd(capture(2*hr, 40_000), "rejected")}, StatusCaptured, 0, 40_000, 400},
		{"concurrent captures post once", []step{upd(capture(hr, 40_000), "ok"), upd(capture(hr, 30_000), "rejected")}, StatusCaptured, 0, 40_000, 400},
		{"capture signal during capture update ignored", []step{upd(capture(hr, 40_000), "ok"), sig(capture(hr, 30_000))}, StatusCaptured, 0, 40_000, 400},
		{"capture during reverse rejected", []step{upd(reverse(hr), "ok"), upd(capture(hr, 40_000), "rejected")}, StatusReversed, 0, 0, 0},
		{"reverse after capture rejected", []step{upd(capture(hr, 40_000), "ok"), upd(reverse(2*hr), "rejected")}, StatusCaptured, 0, 40_000, 400},
		{"refund before capture rejected", []step{upd(refund(hr, "r1", 100), "rejected"), upd(capture(2*hr, 40_000), "ok")}, StatusCaptured, 0, 40_000, 400},
		{"refund", []step{upd(capture(hr, 40_000), "ok"), upd(refund(2*hr, "r1", 15_000), "ok")}, StatusCaptured, 15_000, 25_000, 250},
		{"over-refund rejected", []step{upd(capture(hr, 25_000), "ok"), upd(refund(2*hr, "r1", 30_000), "rejected")}, StatusCaptured, 0, 25_000, 250},
		{"used refund ID rejected", []step{upd(capture(hr, 40_000), "ok"), upd(refund(2*hr, "r1", 15_000), "ok"), upd(refund(3*hr, "r1", 15_000), "rejected")}, StatusCaptured, 15_000, 25_000, 250},
		{"concurrent refunds can't over-refund", []step{upd(capture(hr, 40_000), "ok"), upd(refund(2*hr, "r1", 30_000), "ok"), upd(refund(2*hr, "r2", 20_000), "rejected")}, StatusCaptured, 30_000, 10_000, 100},
		{"concurrent refunds to zero", []step{upd(capture(hr, 200), "ok"), upd(refund(2*hr, "r1", 150), "ok"), upd(refund(2*hr, "r2", 50), "ok")}, StatusRefunded, 200, 0, 0},
		{"split shipment", []step{upd(partial(hr, 15_000), "ok"), upd(capture(2*hr, 25_000), "ok")}, StatusCaptured, 0, 40_000, 400},
		{"short final capture releases the rest", []step{upd(partial(hr, 10_000), "ok"), upd(capture(2*hr, 5_000), "ok")}, StatusCaptured, 0, 15_000, 150},
		{"partials can't exceed the hold", []step{upd(partial(hr, 30_000), "ok"), upd(partial(2*hr, 20_000), "rejected"), upd(capture(3*hr, 10_000), "ok")}, StatusCaptured, 0, 40_000, 400},
		{"expiry after a partial releases the rest", []step{upd(partial(hr, 15_000), "ok")}, StatusCaptured, 0, 15_000, 150},
		{"reverse after a partial releases the rest", []step{upd(partial(hr, 15_000), "ok"), upd(reverse(2*hr), "ok")}, StatusCaptured, 0, 15_000, 150},
		{"partial capture signal", []step{sig(partial(hr, 15_000)), sig(capture(2*hr, 5_000))}, StatusCaptured, 0, 20_000, 200},
		{"refund across a split capture", []step{upd(partial(hr, 15_000), "ok"), upd(capture(2*hr, 25_000), "ok"), upd(refund(3*hr, "r1", 40_000), "ok")}, StatusRefunded, 40_000, 0, 0},
		{"increment then capture", []step{upd(increment(hr, "i1", 30_000), "ok"), upd(capture(2*hr, 70_000), "ok")}, StatusCaptured, 0, 70_000, 700},
		{"increment after a partial", []step{upd(partial(hr, 40_000), "ok"), upd(increment(2*hr, "i1", 10_000), "ok"), upd(capture(3*hr, 10_000), "ok")}, StatusCaptured, 0, 50_000, 500},
		{"capture during increment rejected", []step{upd(increment(hr, "i1", 30_000), "ok"), upd(capture(hr, 40_000), "rejected"), upd(capture(2*hr, 70_000), "ok")}, StatusCaptured, 0, 70_000, 700},
		{"increment over limit declined", []step{upd(increment(hr, "i1", 60_001), "declined"), upd(capture(2*hr, 40_001), "rejected"), upd(capture(3*hr, 40_000), "ok")}, StatusCaptured, 0, 40_000, 400},
		{"increment ID reused rejected", []step{upd(increment(hr, "i1", 1_000), "ok"), upd(increment(2*hr, "i1", 1_000), "rejected"), upd(capture(3*hr, 41_000), "ok")}, StatusCaptured, 0, 41_000, 410},
		{"increment after final capture rejected", []step{upd(capture(hr, 40_000), "ok"), upd(increment(2*hr, "i1", 1_000), "rejected")}, StatusCaptured, 0, 40_000, 400},
		{"refund signal and update share IDs", []step{upd(capture(hr, 40_000), "ok"), sig(refund(2*hr, "r1", 15_000)), upd(refund(3*hr, "r1", 15_000), "rejected")}, StatusCaptured, 15_000, 25_000, 250},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var s testsuite.WorkflowTestSuite
			env := s.NewTestWorkflowEnvironment()
			l := NewMemLedger()
			env.RegisterActivity(&Activities{Ledger: l, Config: card1()})
			got := sendSteps(env, tt.steps)

			env.ExecuteWorkflow(AuthorizeWorkflow, auth("card-1", 40_000))

			if err := env.GetWorkflowError(); err != nil {
				t.Fatal(err)
			}
			checkSteps(t, tt.steps, got)
			var res AuthResult
			if err := env.GetWorkflowResult(&res); err != nil {
				t.Fatal(err)
			}
			if res.Status != tt.want || res.Refunded != tt.wantRefunded || res.Points != tt.wantPoints {
				t.Errorf("result = %+v, want %s refunded %d points %d", res, tt.want, tt.wantRefunded, tt.wantPoints)
			}
			if got := balance(t, l, postedAccount("card-1")); got != tt.wantPosted {
				t.Errorf("posted = %d, want %d", got, tt.wantPosted)
			}
			if got := balance(t, l, holdsAccount("card-1")); got != 0 {
				t.Errorf("holds = %d, want 0", got)
			}
			if got := balance(t, l, pointsAccount("card-1")); got != tt.wantPoints {
				t.Errorf("points account = %d, want %d", got, tt.wantPoints)
			}
			if got := l.Total(); got != 0 {
				t.Errorf("ledger Total = %d, want 0", got)
			}
		})
	}
}

// More than VelocityMax attempts on a card within VelocityWindow declines,
// even with plenty of limit left; older attempts don't count.
func TestVelocityCheck(t *testing.T) {
	tests := []struct {
		name  string
		prior int           // earlier attempts on card-1
		age   time.Duration // how long ago they were
		want  AuthResult
	}{
		{"under the cap", VelocityMax - 1, 0, AuthResult{Status: StatusExpired}},
		{"over the cap declines", VelocityMax, 0, AuthResult{Status: StatusDeclined, Reason: DeclineVelocity}},
		{"old attempts age out", VelocityMax, VelocityWindow + time.Second, AuthResult{Status: StatusExpired}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := NewMemLedger()
			for i := range tt.prior {
				if _, err := l.RecordAttempt(t.Context(), "card-1", fmt.Sprint("earlier-", i), time.Now().Add(-tt.age), VelocityWindow); err != nil {
					t.Fatal(err)
				}
			}
			var s testsuite.WorkflowTestSuite
			env := s.NewTestWorkflowEnvironment()
			env.RegisterActivity(&Activities{Ledger: l, Config: card1()})

			env.ExecuteWorkflow(AuthorizeWorkflow, auth("card-1", 100))

			var res AuthResult
			if err := env.GetWorkflowResult(&res); err != nil {
				t.Fatal(err)
			}
			if res != tt.want {
				t.Errorf("result = %+v, want %+v", res, tt.want)
			}
		})
	}
}

// Rewards come from the partner's configured rate; a merchant without one
// earns the default, and a configured 0 means no points.
func TestRewardRateFromConfig(t *testing.T) {
	tests := []struct {
		name  string
		rates map[string]int64
		want  int64 // points for a $400 capture at "shop"
	}{
		{"no partner row earns the default", nil, 400},
		{"partner rate", map[string]int64{"shop": 3}, 1_200},
		{"a rate of zero earns nothing", map[string]int64{"shop": 0}, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var s testsuite.WorkflowTestSuite
			env := s.NewTestWorkflowEnvironment()
			env.RegisterActivity(&Activities{Ledger: NewMemLedger(), Config: StaticConfig{Limits: card1().Limits, Rates: tt.rates}})
			env.RegisterDelayedCallback(func() { env.SignalWorkflow(CaptureSignal, Capture{Amount: 40_000}) }, hr)

			env.ExecuteWorkflow(AuthorizeWorkflow, auth("card-1", 40_000))

			var res AuthResult
			if err := env.GetWorkflowResult(&res); err != nil {
				t.Fatal(err)
			}
			if res.Points != tt.want {
				t.Errorf("points = %d, want %d", res.Points, tt.want)
			}
		})
	}
}

// brokenConfig is a config store that's down.
type brokenConfig struct{}

func (brokenConfig) CreditLimit(context.Context, string) (int64, bool, error) {
	return 0, false, errors.New("config store down")
}
func (brokenConfig) RewardRate(context.Context, string) (int64, bool, error) {
	return 0, false, errors.New("config store down")
}
func (brokenConfig) APR(context.Context, string) (int64, bool, error) {
	return 0, false, errors.New("config store down")
}
func (brokenConfig) CardStatus(context.Context, string) (string, bool, error) {
	return "", false, errors.New("config store down")
}
func (brokenConfig) CardAccount(context.Context, string) (string, bool, error) {
	return "", false, errors.New("config store down")
}

// A config store outage must fail the auth, not decline the card as unknown.
func TestConfigOutageIsNotADecline(t *testing.T) {
	var s testsuite.WorkflowTestSuite
	env := s.NewTestWorkflowEnvironment()
	env.RegisterActivity(&Activities{Ledger: NewMemLedger(), Config: brokenConfig{}})

	env.ExecuteWorkflow(AuthorizeWorkflow, auth("card-1", 100))

	if err := env.GetWorkflowError(); err == nil || !strings.Contains(err.Error(), "config store down") {
		var res AuthResult
		_ = env.GetWorkflowResult(&res)
		t.Errorf("workflow = %+v, %v; want the config error", res, err)
	}
}

// flakyRates fails its first reward-rate reads, then answers 3 points per $1.
type flakyRates struct {
	StaticConfig
	fails *int
}

func (c flakyRates) RewardRate(ctx context.Context, merchant string) (int64, bool, error) {
	if *c.fails > 0 {
		*c.fails--
		return 0, false, errors.New("config store down")
	}
	return 3, true, nil
}

// A rate read that fails must be retried, not replaced by the default: the
// rate is fixed at settlement, so a wrong one would stick for every refund.
func TestRewardRateOutageRetries(t *testing.T) {
	var s testsuite.WorkflowTestSuite
	env := s.NewTestWorkflowEnvironment()
	fails := 2
	env.RegisterActivity(&Activities{Ledger: NewMemLedger(), Config: flakyRates{card1(), &fails}})
	env.RegisterDelayedCallback(func() { env.SignalWorkflow(CaptureSignal, Capture{Amount: 40_000}) }, hr)

	env.ExecuteWorkflow(AuthorizeWorkflow, auth("card-1", 40_000))

	var res AuthResult
	if err := env.GetWorkflowResult(&res); err != nil {
		t.Fatal(err)
	}
	if res.Points != 1_200 {
		t.Errorf("points = %d, want 1200 (3x on $400, after the store recovers)", res.Points)
	}
}

// A frozen or closed card declines new auths and increments, but a hold
// placed while it was active still settles.
func TestCardStatus(t *testing.T) {
	tests := []struct {
		name     string
		status   string // card-1's status when the auth arrives
		freezeAt time.Duration
		steps    []step
		want     AuthResult
	}{
		{"active", CardActive, 0, []step{upd(capture(hr, 40_000), "ok")}, AuthResult{Status: StatusCaptured, Captured: 40_000, Points: 400}},
		{"frozen declines", CardFrozen, 0, nil, AuthResult{Status: StatusDeclined, Reason: DeclineFrozen}},
		{"closed declines", CardClosed, 0, nil, AuthResult{Status: StatusDeclined, Reason: DeclineClosed}},
		{"frozen after the hold: the capture still settles", CardActive, hr / 2, []step{upd(capture(hr, 40_000), "ok")},
			AuthResult{Status: StatusCaptured, Captured: 40_000, Points: 400}},
		{"frozen after the hold: no increments", CardActive, hr / 2, []step{upd(increment(hr, "i1", 1_000), "declined"), upd(capture(2*hr, 40_000), "ok")},
			AuthResult{Status: StatusCaptured, Captured: 40_000, Points: 400}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var s testsuite.WorkflowTestSuite
			env := s.NewTestWorkflowEnvironment()
			cfg := card1()
			cfg.Statuses = map[string]string{"card-1": tt.status}
			env.RegisterActivity(&Activities{Ledger: NewMemLedger(), Config: cfg})
			if tt.freezeAt > 0 {
				env.RegisterDelayedCallback(func() { cfg.Statuses["card-1"] = CardFrozen }, tt.freezeAt)
			}
			got := sendSteps(env, tt.steps)

			env.ExecuteWorkflow(AuthorizeWorkflow, auth("card-1", 40_000))

			checkSteps(t, tt.steps, got)
			var res AuthResult
			if err := env.GetWorkflowResult(&res); err != nil {
				t.Fatal(err)
			}
			if res != tt.want {
				t.Errorf("result = %+v, want %+v", res, tt.want)
			}
		})
	}
}

// A replacement number authorizes on its account; the number it replaced
// declines without counting as an attempt; an auth already running on the old
// number still settles on the account (SPEC M18).
func TestCardReplacement(t *testing.T) {
	tests := []struct {
		name      string
		number    string        // presented at authorization
		replaceAt time.Duration // card-1 is replaced by card-9 then; 0: before the auth
		steps     []step
		want      AuthResult
	}{
		{"new number authorizes on the account", "card-9", 0, []step{upd(capture(hr, 40_000), "ok")},
			AuthResult{Status: StatusCaptured, Captured: 40_000, Points: 400}},
		{"replaced number declines", "card-1", 0, nil, AuthResult{Status: StatusDeclined, Reason: DeclineReplaced}},
		{"unknown number declines as unknown", "card-x", 0, nil, AuthResult{Status: StatusDeclined, Reason: DeclineUnknownCard}},
		{"auth placed before the replacement still settles", "card-1", hr / 2, []step{upd(capture(hr, 40_000), "ok")},
			AuthResult{Status: StatusCaptured, Captured: 40_000, Points: 400}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var s testsuite.WorkflowTestSuite
			env := s.NewTestWorkflowEnvironment()
			cfg := card1()
			cfg.Numbers, cfg.Replaced = map[string]string{}, map[string]bool{}
			replace := func() { cfg.Numbers["card-9"], cfg.Replaced["card-1"] = "card-1", true }
			if tt.replaceAt == 0 {
				replace()
			} else {
				env.RegisterDelayedCallback(replace, tt.replaceAt)
			}
			l := NewMemLedger()
			env.RegisterActivity(&Activities{Ledger: l, Config: cfg})
			var tagged []string
			env.OnUpsertTypedSearchAttributes(mock.Anything).Run(func(args mock.Arguments) {
				card, _ := args.Get(0).(temporal.SearchAttributes).GetKeyword(CardIDAttribute)
				tagged = append(tagged, card)
			}).Return(nil)
			got := sendSteps(env, tt.steps)

			env.ExecuteWorkflow(AuthorizeWorkflow, auth(tt.number, 40_000))

			checkSteps(t, tt.steps, got)
			var res AuthResult
			if err := env.GetWorkflowResult(&res); err != nil {
				t.Fatal(err)
			}
			if res != tt.want {
				t.Errorf("result = %+v, want %+v", res, tt.want)
			}
			account := "card-1"
			if tt.number == "card-x" {
				account = "card-x"
			}
			if fmt.Sprint(tagged) != "["+account+"]" {
				t.Errorf("CardID upserts = %v, want [%s]: auths are tagged with the account", tagged, account)
			}
			if p := balance(t, l, postedAccount("card-1")); p != tt.want.Captured {
				t.Errorf("card-1 posted = %d, want %d", p, tt.want.Captured)
			}
			accounts, err := l.Accounts(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			for id := range accounts {
				if strings.Contains(id, "card-9") {
					t.Errorf("ledger account %s: a replacement number must post to its account", id)
				}
			}
			// The decline recorded no attempt: five more on the new number all pass velocity.
			if tt.want.Reason == DeclineReplaced {
				n, err := l.RecordAttempt(context.Background(), "card-1", "probe", time.Now(), VelocityWindow)
				if err != nil || n != 1 {
					t.Errorf("attempts on card-1 after the decline = %d (%v), want only the probe's 1", n, err)
				}
			}
		})
	}
}

// Each auth tags itself with its card, so card-scoped visibility finds it.
func TestAuthTagsCardID(t *testing.T) {
	var s testsuite.WorkflowTestSuite
	env := s.NewTestWorkflowEnvironment()
	env.RegisterActivity(&Activities{Ledger: NewMemLedger(), Config: card1()})
	var tagged []string
	env.OnUpsertTypedSearchAttributes(mock.Anything).Run(func(args mock.Arguments) {
		card, _ := args.Get(0).(temporal.SearchAttributes).GetKeyword(CardIDAttribute)
		tagged = append(tagged, card)
	}).Return(nil)

	env.ExecuteWorkflow(AuthorizeWorkflow, auth("card-1", 100_001)) // declined: tagged all the same

	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(tagged) != "[card-1]" {
		t.Errorf("CardID upserts = %v, want [card-1]", tagged)
	}
}

// A partner rate change after settlement must not change what a refund claws
// back, or a full refund would leave the card with negative points.
func TestRewardRateFixedAtSettlement(t *testing.T) {
	var s testsuite.WorkflowTestSuite
	env := s.NewTestWorkflowEnvironment()
	l := NewMemLedger()
	rates := map[string]int64{"shell": 3}
	a := &Activities{Ledger: l, Config: StaticConfig{Limits: card1().Limits, Rates: rates}}
	env.RegisterActivity(a)
	req := AuthRequest{CardID: "card-1", Amount: 10_000, Merchant: "shell"}
	env.RegisterDelayedCallback(func() { env.SignalWorkflow(CaptureSignal, Capture{Amount: 10_000}) }, hr)
	env.RegisterDelayedCallback(func() {
		if got := balance(t, l, pointsAccount("card-1")); got != 300 {
			t.Errorf("points after capture = %d, want 300 (3x on $100)", got)
		}
		rates["shell"] = 10
	}, 90*time.Minute)
	env.RegisterDelayedCallback(func() { env.SignalWorkflow(RefundSignal, Refund{ID: "r1", Amount: 10_000}) }, 2*hr)

	env.ExecuteWorkflow(AuthorizeWorkflow, req)

	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	if got := balance(t, l, pointsAccount("card-1")); got != 0 {
		t.Errorf("points after full refund = %d, want 0", got)
	}
	if got := balance(t, l, partnerPointsAccount("shell")); got != 0 {
		t.Errorf("partner points = %d, want 0", got)
	}
}

// A capture that can't post must release the hold, not strand it.
func TestFailedCaptureReleasesHold(t *testing.T) {
	var s testsuite.WorkflowTestSuite
	env := s.NewTestWorkflowEnvironment()
	l := NewMemLedger()
	a := &Activities{Ledger: l, Config: card1()}
	env.RegisterActivity(a)
	env.OnActivity(a.Settle, mock.Anything, mock.Anything, mock.Anything).
		Return(func(ctx context.Context, req AuthRequest, st Settlement) error {
			if st.Capture > 0 {
				return temporal.NewNonRetryableApplicationError("ledger down", "LedgerDown", nil)
			}
			return a.Settle(ctx, req, st)
		})
	env.RegisterDelayedCallback(func() { env.SignalWorkflow(CaptureSignal, Capture{Amount: 40_000}) }, hr)

	env.ExecuteWorkflow(AuthorizeWorkflow, auth("card-1", 40_000))

	if env.GetWorkflowError() == nil {
		t.Error("want the capture failure surfaced as a workflow error")
	}
	if got := balance(t, l, holdsAccount("card-1")); got != 0 {
		t.Errorf("holds = %d after a failed capture, want 0 (released)", got)
	}
	if got := l.Total(); got != 0 {
		t.Errorf("ledger Total = %d, want 0", got)
	}
}

// A retried activity runs with the same workflow ID, so each step must post once.
func TestActivityRetryPostsOnce(t *testing.T) {
	req := auth("card-1", 2_500)
	tests := []struct {
		name    string
		run     func(*testsuite.TestActivityEnvironment, *Activities) error
		account string
		want    int64
	}{
		{"hold", func(env *testsuite.TestActivityEnvironment, a *Activities) error {
			_, err := env.ExecuteActivity(a.PlaceHold, req)
			return err
		}, holdsAccount("card-1"), 2_500},
		{"capture", func(env *testsuite.TestActivityEnvironment, a *Activities) error {
			_, err := env.ExecuteActivity(a.CaptureHold, req, int64(1_000))
			return err
		}, postedAccount("card-1"), 1_000},
		{"release", func(env *testsuite.TestActivityEnvironment, a *Activities) error {
			_, err := env.ExecuteActivity(a.ReleaseHold, req)
			return err
		}, openToBuyAccount("card-1"), 2_500},
		{"increase hold", func(env *testsuite.TestActivityEnvironment, a *Activities) error {
			_, err := env.ExecuteActivity(a.IncreaseHold, req, Increment{ID: "i1", Amount: 500})
			return err
		}, holdsAccount("card-1"), 500},
		{"settle", func(env *testsuite.TestActivityEnvironment, a *Activities) error {
			_, err := env.ExecuteActivity(a.Settle, req, Settlement{"capture:0", 1_000, 300})
			return err
		}, openToBuyAccount("card-1"), 300},
		{"refund", func(env *testsuite.TestActivityEnvironment, a *Activities) error {
			_, err := env.ExecuteActivity(a.RefundCapture, req, Refund{ID: "r1", Amount: 700})
			return err
		}, postedAccount("card-1"), -700},
		{"rewards", func(env *testsuite.TestActivityEnvironment, a *Activities) error {
			_, err := env.ExecuteActivity(a.AdjustRewards, req, PointsChange{"rewards", 2, 0, 2_500})
			return err
		}, pointsAccount("card-1"), 50},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var s testsuite.WorkflowTestSuite
			env := s.NewTestActivityEnvironment()
			l := NewMemLedger()
			a := &Activities{Ledger: l, Config: card1()}
			env.RegisterActivity(a)
			for range 2 {
				if err := tt.run(env, a); err != nil {
					t.Fatal(err)
				}
			}
			if got := balance(t, l, tt.account); got != tt.want {
				t.Errorf("%s = %d after a retried activity, want %d", tt.account, got, tt.want)
			}
		})
	}
}
