package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
)

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
			env.RegisterActivity(&Activities{Ledger: l, Limits: map[string]int64{"card-1": 100_000}})
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

// A partner rate change after settlement must not change what a refund claws
// back, or a full refund would leave the card with negative points.
func TestRewardRateFixedAtSettlement(t *testing.T) {
	var s testsuite.WorkflowTestSuite
	env := s.NewTestWorkflowEnvironment()
	l := NewMemLedger()
	a := &Activities{Ledger: l, Limits: map[string]int64{"card-1": 100_000}, Rates: map[string]int64{"shell": 3}}
	env.RegisterActivity(a)
	req := AuthRequest{CardID: "card-1", Amount: 10_000, Merchant: "shell"}
	env.RegisterDelayedCallback(func() { env.SignalWorkflow(CaptureSignal, Capture{Amount: 10_000}) }, hr)
	env.RegisterDelayedCallback(func() {
		if got := balance(t, l, pointsAccount("card-1")); got != 300 {
			t.Errorf("points after capture = %d, want 300 (3x on $100)", got)
		}
		a.Rates["shell"] = 10
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
	a := &Activities{Ledger: l, Limits: map[string]int64{"card-1": 100_000}}
	env.RegisterActivity(a)
	env.OnActivity(a.CaptureHold, mock.Anything, mock.Anything, mock.Anything).
		Return(temporal.NewNonRetryableApplicationError("ledger down", "LedgerDown", nil))
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
			a := &Activities{Ledger: l, Limits: map[string]int64{"card-1": 100_000}}
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
