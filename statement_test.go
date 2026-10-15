package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

const day = 24 * time.Hour

// seeded is a ledger where card-1 has already spent cents.
func seeded(t *testing.T, cents int64) *MemLedger {
	t.Helper()
	l := NewMemLedger()
	if err := l.Post(t.Context(), "seed", Entry{postedAccount("card-1"), cents}, Entry{openToBuyAccount("card-1"), -cents}); err != nil {
		t.Fatal(err)
	}
	return l
}

func pay(at time.Duration, id string, amount int64) signal {
	return signal{at, PayUpdate, Payment{ID: id, Amount: amount}}
}

// One run is one cycle: it starts at the last close, takes payments, charges a
// late fee at the due date and interest at the next close, then continues as
// new with the statement it just closed.
func TestStatementCycle(t *testing.T) {
	stmt := Statement{N: 1, Balance: 50_000, MinDue: 2_500}
	half := 12 * time.Hour // payments land mid-day, never on a sample
	// Interest is Σ end-of-day balances × 24.99% / 365, over 30 daily samples.
	tests := []struct {
		name       string
		last       Statement
		seedPosted int64
		steps      []step
		want       Statement // the next statement
	}{
		{"first close: no interest", Statement{}, 50_000, nil, Statement{N: 1, Balance: 50_000, MinDue: 2_500}},
		{"min due is 2% of a large balance", Statement{}, 500_000, nil, Statement{N: 1, Balance: 500_000, MinDue: 10_000}},
		{"min due is a small balance", Statement{}, 1_000, nil, Statement{N: 1, Balance: 1_000, MinDue: 1_000}},
		{"paid in full: no fee, no interest", stmt, 50_000, []step{upd(pay(9*day+half, "p1", 50_000), "ok")}, Statement{N: 2}},
		// 9 days at 50,000 + 21 at 47,500 = 1,447,500 → 991.04
		{"minimum paid: interest on the daily balance", stmt, 50_000, []step{upd(pay(9*day+half, "p1", 2_500), "ok")}, Statement{N: 2, Balance: 48_491, MinDue: 2_500, Interest: 991}},
		// fee on day 25, before its sample: 24 days at 50,000 + 6 at 52,500 = 1,515,000 → 1,037.25
		{"nothing paid: fee and interest", stmt, 50_000, nil, Statement{N: 2, Balance: 53_537, MinDue: 2_500, Fees: 2_500, Interest: 1_037}},
		{"paid in full after the due date: fee only", stmt, 50_000, []step{upd(pay(27*day+half, "p1", 50_000), "ok")}, Statement{N: 2, Balance: 2_500, MinDue: 2_500, Fees: 2_500}},
		{"overpaid: credit balance, nothing due", stmt, 50_000, []step{upd(pay(9*day+half, "p1", 60_000), "ok")}, Statement{N: 2, Balance: -10_000}},
		{"payment ID reused rejected", stmt, 50_000, []step{upd(pay(9*day+half, "p1", 2_500), "ok"), upd(pay(10*day+half, "p1", 2_500), "rejected")}, Statement{N: 2, Balance: 48_491, MinDue: 2_500, Interest: 991}},
		{"non-positive payment rejected", stmt, 50_000, []step{upd(pay(9*day+half, "p1", 0), "rejected"), upd(pay(9*day+half, "p2", 2_500), "ok")}, Statement{N: 2, Balance: 48_491, MinDue: 2_500, Interest: 991}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var s testsuite.WorkflowTestSuite
			env := s.NewTestWorkflowEnvironment()
			l := seeded(t, tt.seedPosted)
			env.RegisterActivity(&Activities{Ledger: l, Config: StaticConfig{}})
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
	l := seeded(t, 50_000)
	a := &Activities{Ledger: l, Config: StaticConfig{}}
	env.RegisterActivity(a)
	// A payment takes two days to post.
	env.OnActivity(a.Pay, mock.Anything, mock.Anything, mock.Anything, mock.Anything).After(2 * day).Return(a.Pay)
	steps := []step{
		upd(pay(29*day+12*time.Hour, "in-flight", 1_000), "ok"),       // still posting at the day-30 close
		upd(pay(30*day+12*time.Hour, "mid-close", 1_000), "rejected"), // arrives while the close waits for it
	}
	got := sendSteps(env, steps)

	env.ExecuteWorkflow(StatementWorkflow, StatementTerms{CardID: "card-1"}, Statement{N: 1, Balance: 50_000, MinDue: 2_500})

	checkSteps(t, steps, got)
	_, next := continuedAs(t, env)
	// Nothing paid by day 25, so the fee lands; the payment posts after the last
	// sample: 24 days at 50,000 + 6 at 52,500 = 1,515,000 → 1,037.25 interest.
	if want := (Statement{N: 2, Balance: 52_537, MinDue: 2_500, Fees: 2_500, Interest: 1_037}); next != want {
		t.Errorf("next statement = %+v, want %+v", next, want)
	}
	if next.Balance != balance(t, l, postedAccount("card-1")) {
		t.Errorf("statement balance %d, posted %d", next.Balance, balance(t, l, postedAccount("card-1")))
	}
}

// A payment resent in a later cycle posts nothing and doesn't count toward
// that cycle's minimum, so the late fee still lands.
func TestStatementPaymentResentNextCycle(t *testing.T) {
	l := seeded(t, 50_000)
	cycle := func(last Statement, steps []step) Statement {
		var s testsuite.WorkflowTestSuite
		env := s.NewTestWorkflowEnvironment() // same workflow ID every run, as after ContinueAsNew
		env.RegisterActivity(&Activities{Ledger: l, Config: StaticConfig{}})
		got := sendSteps(env, steps)
		env.ExecuteWorkflow(StatementWorkflow, StatementTerms{CardID: "card-1"}, last)
		checkSteps(t, steps, got)
		_, next := continuedAs(t, env)
		return next
	}
	s2 := cycle(Statement{N: 1, Balance: 50_000, MinDue: 2_500}, []step{upd(pay(9*day+12*time.Hour, "p1", 2_500), "ok")})
	s3 := cycle(s2, []step{upd(pay(9*day+12*time.Hour, "p1", 2_500), "ok")}) // the resend

	// The resend counts for nothing, so the fee lands on day 25:
	// 24 days at 48,491 + 6 at 50,991 = 1,469,730 → 1,006.26 interest.
	if want := (Statement{N: 3, Balance: 51_997, MinDue: 2_500, Fees: 2_500, Interest: 1_006}); s3 != want {
		t.Errorf("statement 3 = %+v, want %+v", s3, want)
	}
	if got := balance(t, l, postedAccount("card-1")); got != s3.Balance {
		t.Errorf("posted = %d, want %d", got, s3.Balance)
	}
	if got := l.Total(); got != 0 {
		t.Errorf("ledger Total = %d, want 0", got)
	}
}

// The APR comes from the card's config at close; a card without one gets the default.
func TestStatementAPRFromConfig(t *testing.T) {
	tests := []struct {
		name string
		aprs map[string]int64
		want int64 // interest on 9 days at 50,000 + 21 at 47,500 = 1,447,500
	}{
		{"card's own APR", map[string]int64{"card-1": 1_200}, 475}, // × 12% / 365 = 475.89
		{"default APR", nil, 991},                                  // × 24.99% / 365 = 991.04
		{"zero APR", map[string]int64{"card-1": 0}, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var s testsuite.WorkflowTestSuite
			env := s.NewTestWorkflowEnvironment()
			l := seeded(t, 50_000)
			env.RegisterActivity(&Activities{Ledger: l, Config: StaticConfig{APRs: tt.aprs}})
			sendSteps(env, []step{upd(pay(9*day+12*time.Hour, "p1", 2_500), "ok")})

			env.ExecuteWorkflow(StatementWorkflow, StatementTerms{CardID: "card-1"}, Statement{N: 1, Balance: 50_000, MinDue: 2_500})

			if _, next := continuedAs(t, env); next.Interest != tt.want {
				t.Errorf("interest = %d, want %d", next.Interest, tt.want)
			}
		})
	}
}

// Days on a credit balance accrue nothing; they don't offset days that owe.
func TestStatementCreditDaysCountAsZero(t *testing.T) {
	var s testsuite.WorkflowTestSuite
	env := s.NewTestWorkflowEnvironment()
	l := seeded(t, 50_000)
	env.RegisterActivity(&Activities{Ledger: l, Config: StaticConfig{}})
	sendSteps(env, []step{upd(pay(9*day+12*time.Hour, "p1", 2_500), "ok")})
	env.RegisterDelayedCallback(func() { // a $600 merchant refund: the card goes into credit
		if err := l.Post(t.Context(), "refund", Entry{postedAccount("card-1"), -60_000}, Entry{openToBuyAccount("card-1"), 60_000}); err != nil {
			t.Error(err)
		}
	}, 14*day+12*time.Hour)

	env.ExecuteWorkflow(StatementWorkflow, StatementTerms{CardID: "card-1"}, Statement{N: 1, Balance: 50_000, MinDue: 2_500})

	// 9 days at 50,000 + 5 at 47,500 + 16 at a 12,500 credit (as 0) = 687,500 → 470.70
	if _, next := continuedAs(t, env); next != (Statement{N: 2, Balance: -12_030, Interest: 470}) {
		t.Errorf("next statement = %+v, want interest 470 on a -12,030 balance", next)
	}
}

// flakyBalances fails its first balance reads, as if the ledger were down.
type flakyBalances struct {
	Ledger
	fails *int
}

func (f flakyBalances) Balance(ctx context.Context, account string) (int64, error) {
	if *f.fails > 0 {
		*f.fails--
		return 0, errors.New("ledger down")
	}
	return f.Ledger.Balance(ctx, account)
}

// An outage longer than a saga's bounded retries must not end a card's
// billing: the statement's own ledger steps retry until the ledger answers.
func TestStatementRidesOutLedgerOutage(t *testing.T) {
	var s testsuite.WorkflowTestSuite
	env := s.NewTestWorkflowEnvironment()
	fails := 8 // past withLedger's 5 attempts; the test env caps "unlimited" at 10
	l := NewMemLedger()
	env.RegisterActivity(&Activities{Ledger: flakyBalances{l, &fails}, Config: StaticConfig{}})

	env.ExecuteWorkflow(StatementWorkflow, StatementTerms{CardID: "card-1"}, Statement{})

	if _, next := continuedAs(t, env); next.N != 1 {
		t.Errorf("next statement = %+v, want statement 1", next)
	}
	if fails != 0 {
		t.Errorf("%d failures left; want the outage ridden out", fails)
	}
}

// The statement query shows payments as they land.
func TestStatementQuery(t *testing.T) {
	var s testsuite.WorkflowTestSuite
	env := s.NewTestWorkflowEnvironment()
	env.RegisterActivity(&Activities{Ledger: NewMemLedger(), Config: StaticConfig{}})
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
