package main

import (
	"testing"

	"go.temporal.io/sdk/testsuite"
)

func TestAuthorizeWorkflow(t *testing.T) {
	tests := []struct {
		name        string
		heldAlready int64 // existing holds on card-1 before this auth
		req         AuthRequest
		want        string // approve, decline, or error
		wantHolds   int64  // card-1 holds balance afterwards
	}{
		{"within limit", 0, AuthRequest{"card-1", 40_000, "shop"}, "approve", 40_000},
		{"exactly the limit", 0, AuthRequest{"card-1", 100_000, "shop"}, "approve", 100_000},
		{"over limit", 0, AuthRequest{"card-1", 100_001, "shop"}, "decline", 0},
		{"over limit after earlier hold", 70_000, AuthRequest{"card-1", 40_000, "shop"}, "decline", 70_000},
		{"unknown card", 0, AuthRequest{"card-x", 100, "shop"}, "decline", 0},
		{"non-positive amount", 0, AuthRequest{"card-1", 0, "shop"}, "error", 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var s testsuite.WorkflowTestSuite
			env := s.NewTestWorkflowEnvironment()
			l := NewLedger()
			if err := l.Post("seed", Entry{holdsAccount("card-1"), tt.heldAlready}, Entry{openToBuyAccount("card-1"), -tt.heldAlready}); err != nil {
				t.Fatal(err)
			}
			env.RegisterActivity(&Activities{Ledger: l, Limits: map[string]int64{"card-1": 100_000}})

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
				got = map[bool]string{true: "approve", false: "decline"}[res.Approved]
			}
			if got != tt.want {
				t.Errorf("outcome = %s, want %s (err %v)", got, tt.want, env.GetWorkflowError())
			}
			if got := l.Balance(holdsAccount("card-1")); got != tt.wantHolds {
				t.Errorf("holds = %d, want %d", got, tt.wantHolds)
			}
			if got := l.Total(); got != 0 {
				t.Errorf("ledger Total = %d, want 0", got)
			}
		})
	}
}

// A retried PlaceHold runs with the same workflow ID, so it must post once.
func TestPlaceHoldRetryPostsOnce(t *testing.T) {
	var s testsuite.WorkflowTestSuite
	env := s.NewTestActivityEnvironment()
	l := NewLedger()
	a := &Activities{Ledger: l}
	env.RegisterActivity(a)

	req := AuthRequest{"card-1", 2_500, "shop"}
	for range 2 {
		if _, err := env.ExecuteActivity(a.PlaceHold, req); err != nil {
			t.Fatal(err)
		}
	}
	if got := l.Balance(holdsAccount("card-1")); got != 2_500 {
		t.Errorf("holds = %d after a retried PlaceHold, want 2500", got)
	}
}
