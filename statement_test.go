package main

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

const day = 24 * time.Hour

func pay(at time.Duration, id string, amount int64) signal {
	return signal{at, PayUpdate, Payment{ID: id, Amount: amount}}
}

// One run is one cycle: it starts at the last close, takes payments, charges a
// late fee at the due date and interest at the next close, then continues as
// new with the statement it just closed.
func TestStatementCycle(t *testing.T) {
	stmt := Statement{N: 1, Balance: 50_000, MinDue: 2_500}
	tests := []struct {
		name       string
		last       Statement
		seedPosted int64
		steps      []step
		want       Statement // the next statement
	}{
		{"first close", Statement{}, 50_000, nil, Statement{N: 1, Balance: 50_000, MinDue: 2_500}},
		{"min due is 2% of a large balance", Statement{}, 500_000, nil, Statement{N: 1, Balance: 500_000, MinDue: 10_000}},
		{"min due is a small balance", Statement{}, 1_000, nil, Statement{N: 1, Balance: 1_000, MinDue: 1_000}},
		{"paid in full: no fee, no interest", stmt, 50_000, []step{upd(pay(10*day, "p1", 50_000), "ok")}, Statement{N: 2}},
		// 47,500 unpaid × 24.99% / 12 = 989.27
		{"minimum paid: interest only", stmt, 50_000, []step{upd(pay(10*day, "p1", 2_500), "ok")}, Statement{N: 2, Balance: 48_489, MinDue: 2_500, Interest: 989}},
		{"nothing paid: fee and interest", stmt, 50_000, nil, Statement{N: 2, Balance: 53_541, MinDue: 2_500, Fees: 2_500, Interest: 1_041}},
		{"paid in full after the due date: fee only", stmt, 50_000, []step{upd(pay(27*day, "p1", 50_000), "ok")}, Statement{N: 2, Balance: 2_500, MinDue: 2_500, Fees: 2_500}},
		{"overpaid: credit balance, nothing due", stmt, 50_000, []step{upd(pay(10*day, "p1", 60_000), "ok")}, Statement{N: 2, Balance: -10_000}},
		{"payment ID reused rejected", stmt, 50_000, []step{upd(pay(10*day, "p1", 2_500), "ok"), upd(pay(11*day, "p1", 2_500), "rejected")}, Statement{N: 2, Balance: 48_489, MinDue: 2_500, Interest: 989}},
		{"non-positive payment rejected", stmt, 50_000, []step{upd(pay(10*day, "p1", 0), "rejected"), upd(pay(10*day, "p2", 2_500), "ok")}, Statement{N: 2, Balance: 48_489, MinDue: 2_500, Interest: 989}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var s testsuite.WorkflowTestSuite
			env := s.NewTestWorkflowEnvironment()
			l := NewMemLedger()
			if err := l.Post(t.Context(), "seed", Entry{postedAccount("card-1"), tt.seedPosted}, Entry{openToBuyAccount("card-1"), -tt.seedPosted}); err != nil {
				t.Fatal(err)
			}
			env.RegisterActivity(&Activities{Ledger: l})
			got := sendSteps(env, tt.steps)

			env.ExecuteWorkflow(StatementWorkflow, StatementTerms{CardID: "card-1"}, tt.last)

			checkSteps(t, tt.steps, got)
			terms, next := continuedAs(t, env)
			if terms.CardID != "card-1" {
				t.Errorf("terms not carried: %+v", terms)
			}
			if next != tt.want {
				t.Errorf("next statement = %+v, want %+v", next, tt.want)
			}
			if got := balance(t, l, postedAccount("card-1")); got != tt.want.Balance {
				t.Errorf("posted = %d, want the statement balance %d", got, tt.want.Balance)
			}
			if got := l.Total(); got != 0 {
				t.Errorf("ledger Total = %d, want 0", got)
			}
		})
	}
}

// A statement closes on settled payments: one still posting at the close
// finishes first, and one arriving mid-close is turned away to retry.
func TestStatementCloseWithPaymentsInFlight(t *testing.T) {
	var s testsuite.WorkflowTestSuite
	env := s.NewTestWorkflowEnvironment()
	l := NewMemLedger()
	a := &Activities{Ledger: l}
	env.RegisterActivity(a)
	// Payments and interest each take two days to post.
	env.OnActivity(a.PostToCard, mock.Anything, mock.Anything, mock.Anything, mock.Anything).After(2 * day).Return(a.PostToCard)
	steps := []step{
		upd(pay(29*day, "in-flight", 1_000), "ok"),       // still posting at the day-30 close
		upd(pay(31*day, "mid-close", 1_000), "rejected"), // arrives while interest posts
	}
	got := sendSteps(env, steps)

	env.ExecuteWorkflow(StatementWorkflow, StatementTerms{CardID: "card-1"}, Statement{N: 1, Balance: 50_000, MinDue: 2_500})

	checkSteps(t, steps, got)
	_, next := continuedAs(t, env)
	if next.Interest != 1_020 { // on 49,000: the in-flight payment counts
		t.Errorf("interest = %d, want 1020", next.Interest)
	}
	if next.Balance != balance(t, l, postedAccount("card-1")) {
		t.Errorf("statement balance %d, posted %d", next.Balance, balance(t, l, postedAccount("card-1")))
	}
}

// A payment resent in a later cycle posts nothing and doesn't count toward
// that cycle's minimum, so the late fee still lands.
func TestStatementPaymentResentNextCycle(t *testing.T) {
	l := NewMemLedger()
	if err := l.Post(t.Context(), "seed", Entry{postedAccount("card-1"), 50_000}, Entry{openToBuyAccount("card-1"), -50_000}); err != nil {
		t.Fatal(err)
	}
	cycle := func(last Statement, steps []step) Statement {
		var s testsuite.WorkflowTestSuite
		env := s.NewTestWorkflowEnvironment() // same workflow ID every run, as after ContinueAsNew
		env.RegisterActivity(&Activities{Ledger: l})
		got := sendSteps(env, steps)
		env.ExecuteWorkflow(StatementWorkflow, StatementTerms{CardID: "card-1"}, last)
		checkSteps(t, steps, got)
		_, next := continuedAs(t, env)
		return next
	}
	s2 := cycle(Statement{N: 1, Balance: 50_000, MinDue: 2_500}, []step{upd(pay(10*day, "p1", 2_500), "ok")})
	s3 := cycle(s2, []step{upd(pay(10*day, "p1", 2_500), "ok")}) // the resend

	// 48,489 + 25.00 fee + 48,489 × 24.99% / 12 = 1,009 interest
	if want := (Statement{N: 3, Balance: 51_998, MinDue: 2_500, Fees: 2_500, Interest: 1_009}); s3 != want {
		t.Errorf("statement 3 = %+v, want %+v", s3, want)
	}
	if got := balance(t, l, postedAccount("card-1")); got != s3.Balance {
		t.Errorf("posted = %d, want %d", got, s3.Balance)
	}
	if got := l.Total(); got != 0 {
		t.Errorf("ledger Total = %d, want 0", got)
	}
}

// The statement query shows payments as they land.
func TestStatementQuery(t *testing.T) {
	var s testsuite.WorkflowTestSuite
	env := s.NewTestWorkflowEnvironment()
	env.RegisterActivity(&Activities{Ledger: NewMemLedger()})
	last := Statement{N: 1, Balance: 50_000, MinDue: 2_500}
	env.RegisterDelayedCallback(func() {
		env.UpdateWorkflow(PayUpdate, "1", &testsuite.TestUpdateCallback{OnComplete: func(any, error) {}}, Payment{ID: "p1", Amount: 3_000})
	}, 10*day)
	env.RegisterDelayedCallback(func() {
		v, err := env.QueryWorkflow(StatementQuery)
		if err != nil {
			t.Fatal(err)
		}
		var got Statement
		if err := v.Get(&got); err != nil {
			t.Fatal(err)
		}
		if want := (Statement{N: 1, Balance: 50_000, MinDue: 2_500, Paid: 3_000}); got != want {
			t.Errorf("statement = %+v, want %+v", got, want)
		}
	}, 11*day)

	env.ExecuteWorkflow(StatementWorkflow, StatementTerms{CardID: "card-1"}, last)

	continuedAs(t, env)
}

// continuedAs returns the arguments the run continued as new with.
func continuedAs(t *testing.T, env *testsuite.TestWorkflowEnvironment) (StatementTerms, Statement) {
	t.Helper()
	var can *workflow.ContinueAsNewError
	if !errors.As(env.GetWorkflowError(), &can) {
		t.Fatalf("want continue-as-new, got %v", env.GetWorkflowError())
	}
	var terms StatementTerms
	var next Statement
	if err := converter.GetDefaultDataConverter().FromPayloads(can.Input, &terms, &next); err != nil {
		t.Fatal(err)
	}
	return terms, next
}
