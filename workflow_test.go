package main

import (
	"testing"
	"time"

	"go.temporal.io/sdk/testsuite"
)

func auth(card string, amount int64) AuthRequest {
	return AuthRequest{CardID: card, Amount: amount, Merchant: "shop"}
}

func TestAuthorizeWorkflow(t *testing.T) {
	short := auth("card-1", 40_000)
	short.HoldFor = 30 * time.Minute // expires before the 1h capture arrives

	tests := []struct {
		name       string
		seedHolds  int64 // card-1 balances before this auth
		seedPosted int64
		req        AuthRequest
		captures   []int64 // capture signals sent 1h into the hold
		want       string  // a Status, or "error"
		wantHolds  int64   // card-1 balances afterwards
		wantPosted int64
	}{
		{"full capture", 0, 0, auth("card-1", 40_000), []int64{40_000}, StatusCaptured, 0, 40_000},
		{"partial capture releases the rest", 0, 0, auth("card-1", 40_000), []int64{25_000}, StatusCaptured, 0, 25_000},
		{"no capture expires", 0, 0, auth("card-1", 40_000), nil, StatusExpired, 0, 0},
		{"hold expires before capture", 0, 0, short, []int64{40_000}, StatusExpired, 0, 0},
		{"invalid captures ignored", 0, 0, auth("card-1", 40_000), []int64{50_000, 0, 30_000}, StatusCaptured, 0, 30_000},
		{"duplicate capture posts once", 0, 0, auth("card-1", 40_000), []int64{40_000, 40_000}, StatusCaptured, 0, 40_000},
		{"exactly the limit", 0, 0, auth("card-1", 100_000), []int64{100_000}, StatusCaptured, 0, 100_000},
		{"over limit", 0, 0, auth("card-1", 100_001), nil, StatusDeclined, 0, 0},
		{"over limit after earlier hold", 70_000, 0, auth("card-1", 40_000), nil, StatusDeclined, 70_000, 0},
		{"over limit after earlier spend", 0, 70_000, auth("card-1", 40_000), nil, StatusDeclined, 0, 70_000},
		{"unknown card", 0, 0, auth("card-x", 100), nil, StatusDeclined, 0, 0},
		{"non-positive amount", 0, 0, auth("card-1", 0), nil, "error", 0, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var s testsuite.WorkflowTestSuite
			env := s.NewTestWorkflowEnvironment()
			l := NewLedger()
			if err := l.Post("seed",
				Entry{holdsAccount("card-1"), tt.seedHolds},
				Entry{postedAccount("card-1"), tt.seedPosted},
				Entry{openToBuyAccount("card-1"), -tt.seedHolds - tt.seedPosted},
			); err != nil {
				t.Fatal(err)
			}
			env.RegisterActivity(&Activities{Ledger: l, Limits: map[string]int64{"card-1": 100_000}})
			env.RegisterDelayedCallback(func() {
				for _, amt := range tt.captures {
					env.SignalWorkflow(CaptureSignal, Capture{Amount: amt})
				}
			}, time.Hour)

			env.ExecuteWorkflow(AuthorizeWorkflow, tt.req)

			if !env.IsWorkflowCompleted() {
				t.Fatal("workflow did not complete")
			}
			got := "error"
			if env.GetWorkflowError() == nil {
				var res AuthResult
				if err := env.GetWorkflowResult(&res); err != nil {
					t.Fatal(err)
				}
				got = res.Status
			}
			if got != tt.want {
				t.Errorf("outcome = %s, want %s (err %v)", got, tt.want, env.GetWorkflowError())
			}
			if got := l.Balance(holdsAccount("card-1")); got != tt.wantHolds {
				t.Errorf("holds = %d, want %d", got, tt.wantHolds)
			}
			if got := l.Balance(postedAccount("card-1")); got != tt.wantPosted {
				t.Errorf("posted = %d, want %d", got, tt.wantPosted)
			}
			if got := l.Total(); got != 0 {
				t.Errorf("ledger Total = %d, want 0", got)
			}
		})
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
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var s testsuite.WorkflowTestSuite
			env := s.NewTestActivityEnvironment()
			l := NewLedger()
			a := &Activities{Ledger: l}
			env.RegisterActivity(a)
			for range 2 {
				if err := tt.run(env, a); err != nil {
					t.Fatal(err)
				}
			}
			if got := l.Balance(tt.account); got != tt.want {
				t.Errorf("%s = %d after a retried activity, want %d", tt.account, got, tt.want)
			}
		})
	}
}
