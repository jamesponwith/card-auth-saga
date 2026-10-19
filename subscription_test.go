package main

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

func approve() Decision              { return Decision{Approved: true} }
func decline(reason string) Decision { return Decision{Reason: reason} }

// One run is one cycle (SPEC M19): it charges at the cycle start, retries a
// soft decline on days 1, 3 and 7, and then continues as new at the cycle's
// end, lapses, or ends.
func TestSubscriptionCycle(t *testing.T) {
	const cycle = 30 * day
	tests := []struct {
		name      string
		cycle     time.Duration // zero: 30 days
		last      Subscription  // carried from the previous cycle
		decisions []Decision    // ChargeCard's answers, in order
		cancelAt  time.Duration
		wantAt    []time.Duration // when each charge was attempted
		want      Subscription
		continued bool          // continued as new into the next cycle
		endAt     time.Duration // when the run finished
	}{
		{"charged: next cycle at the cycle's end", 0, Subscription{}, []Decision{approve()}, 0,
			[]time.Duration{0}, Subscription{Cycle: 1, Status: SubActive, Charged: 1_000}, true, cycle},
		{"cycle numbers carry on", 0, Subscription{Cycle: 4, Status: SubActive, Charged: 4_000}, []Decision{approve()}, 0,
			[]time.Duration{0}, Subscription{Cycle: 5, Status: SubActive, Charged: 5_000}, true, cycle},
		{"declined, then charged on the second retry", 0, Subscription{}, []Decision{decline(DeclineOverLimit), decline(DeclineFrozen), approve()}, 0,
			[]time.Duration{0, day, 3 * day}, Subscription{Cycle: 1, Status: SubActive, Charged: 1_000}, true, cycle},
		{"lapses when the last retry fails", 0, Subscription{}, []Decision{decline(DeclineOverLimit), decline(DeclineOverLimit), decline(DeclineVelocity), decline(DeclineOverLimit)}, 0,
			[]time.Duration{0, day, 3 * day, 7 * day}, Subscription{Cycle: 1, Status: SubLapsed, Failures: 4, LastReason: DeclineOverLimit}, false, 7 * day},
		{"a closed card ends it", 0, Subscription{}, []Decision{decline(DeclineClosed)}, 0,
			[]time.Duration{0}, Subscription{Cycle: 1, Status: SubEnded, Failures: 1, LastReason: DeclineClosed}, false, 0},
		{"a blocked merchant ends it", 0, Subscription{}, []Decision{decline(DeclineMerchantBlocked)}, 0,
			[]time.Duration{0}, Subscription{Cycle: 1, Status: SubEnded, Failures: 1, LastReason: DeclineMerchantBlocked}, false, 0},
		{"an unknown card ends it", 0, Subscription{}, []Decision{decline(DeclineUnknownCard)}, 0,
			[]time.Duration{0}, Subscription{Cycle: 1, Status: SubEnded, Failures: 1, LastReason: DeclineUnknownCard}, false, 0},
		{"cancelled between charges", 0, Subscription{}, []Decision{approve()}, 5 * day,
			[]time.Duration{0}, Subscription{Cycle: 1, Status: SubCancelled, Charged: 1_000}, false, 5 * day},
		{"retries outlast a short cycle: the next one starts right after the charge", 2 * day, Subscription{}, []Decision{decline(DeclineOverLimit), decline(DeclineOverLimit), approve()}, 0,
			[]time.Duration{0, day, 3 * day}, Subscription{Cycle: 1, Status: SubActive, Charged: 1_000}, true, 3 * day},
		{"cancelled while past due: no more retries", 0, Subscription{}, []Decision{decline(DeclineOverLimit)}, 12 * time.Hour,
			[]time.Duration{0}, Subscription{Cycle: 1, Status: SubCancelled, Failures: 1, LastReason: DeclineOverLimit}, false, 12 * time.Hour},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var s testsuite.WorkflowTestSuite
			env := s.NewTestWorkflowEnvironment()
			env.RegisterActivity(&Activities{Ledger: NewMemLedger(), Config: card1()})
			start := env.Now()
			var at []time.Duration
			var ids []string
			env.OnActivity("ChargeCard", mock.Anything, mock.Anything, mock.Anything).Return(func(_ context.Context, id string, req AuthRequest) (Decision, error) {
				at, ids = append(at, env.Now().Sub(start)), append(ids, id)
				if req != (AuthRequest{CardID: "card-1", Amount: 1_000, Merchant: "streamer"}) {
					return Decision{}, fmt.Errorf("charge %+v, want card-1's 1000 at streamer", req)
				}
				return tt.decisions[len(at)-1], nil
			})
			var steps []step
			if tt.cancelAt > 0 {
				steps = []step{upd(signal{tt.cancelAt, CancelUpdate, nil}, "ok")}
			}
			sent := sendSteps(env, steps)

			env.ExecuteWorkflow(SubscriptionWorkflow, SubscriptionTerms{ID: "s1", CardID: "card-1", Merchant: "streamer", Amount: 1_000, Cycle: tt.cycle}, tt.last)

			checkSteps(t, steps, sent)
			if fmt.Sprint(at) != fmt.Sprint(tt.wantAt) {
				t.Errorf("charges at %v, want %v", at, tt.wantAt)
			}
			for i, id := range ids { // one auth per attempt, so a declined auth is never reused
				if want := fmt.Sprintf("subscription:s1:c%d:a%d", tt.want.Cycle, i); id != want {
					t.Errorf("charge %d auth ID %q, want %q", i, id, want)
				}
			}
			var got Subscription
			if tt.continued {
				var can *workflow.ContinueAsNewError
				if !errors.As(env.GetWorkflowError(), &can) {
					t.Fatalf("want continue-as-new, got %v", env.GetWorkflowError())
				}
				var terms SubscriptionTerms
				if err := converter.GetDefaultDataConverter().FromPayloads(can.Input, &terms, &got); err != nil {
					t.Fatal(err)
				}
				if terms.ID != "s1" || terms.CardID != "card-1" {
					t.Errorf("terms not carried: %+v", terms)
				}
			} else if err := env.GetWorkflowResult(&got); err != nil {
				t.Fatal(err)
			}
			if got != tt.want {
				t.Errorf("subscription = %+v, want %+v", got, tt.want)
			}
			if end := env.Now().Sub(start); end != tt.endAt {
				t.Errorf("run ended at %v, want %v", end, tt.endAt)
			}
		})
	}
}

// The account updater: a charge goes to the card's current number.
func TestCurrentNumber(t *testing.T) {
	cfg := StaticConfig{Numbers: map[string]string{"card-2": "card-1", "card-3": "card-1"}, Replaced: map[string]bool{"card-1": true, "card-2": true}}
	for number, want := range map[string]string{"card-1": "card-3", "card-2": "card-3", "card-3": "card-3", "card-9": "card-9"} {
		if got, err := cfg.CurrentNumber(context.Background(), number); err != nil || got != want {
			t.Errorf("CurrentNumber(%s) = %q, %v; want %q", number, got, err, want)
		}
	}
}

// A subscription tags itself with its card's account, even when signed up on
// a replacement number, so a card's subscriptions can be listed (SPEC M22).
func TestSubscriptionTagsAccount(t *testing.T) {
	var s testsuite.WorkflowTestSuite
	env := s.NewTestWorkflowEnvironment()
	cfg := card1()
	cfg.Numbers = map[string]string{"card-9": "card-1"}
	env.RegisterActivity(&Activities{Ledger: NewMemLedger(), Config: cfg})
	env.OnActivity("ChargeCard", mock.Anything, mock.Anything, mock.Anything).Return(approve(), nil)
	var tagged []string
	env.OnUpsertTypedSearchAttributes(mock.Anything).Run(func(args mock.Arguments) {
		card, _ := args.Get(0).(temporal.SearchAttributes).GetKeyword(CardIDAttribute)
		tagged = append(tagged, card)
	}).Return(nil)

	env.ExecuteWorkflow(SubscriptionWorkflow, SubscriptionTerms{ID: "s1", CardID: "card-9", Merchant: "streamer", Amount: 1_000}, Subscription{})

	if fmt.Sprint(tagged) != "[card-1]" {
		t.Errorf("CardID upserts = %v, want [card-1]", tagged)
	}
}

// The query answers with the terms and the state, and still decodes into a
// Subscription alone. Both types have a Cycle: neither may be dropped.
func TestSubscriptionQuery(t *testing.T) {
	var s testsuite.WorkflowTestSuite
	env := s.NewTestWorkflowEnvironment()
	env.RegisterActivity(&Activities{Ledger: NewMemLedger(), Config: card1()})
	env.OnActivity("ChargeCard", mock.Anything, mock.Anything, mock.Anything).Return(approve(), nil)
	terms := SubscriptionTerms{ID: "s1", CardID: "card-1", Merchant: "streamer", Amount: 1_000, Cycle: 2 * day}
	env.RegisterDelayedCallback(func() {
		v, err := env.QueryWorkflow(SubscriptionQuery)
		var view SubscriptionView
		var sub Subscription
		if err == nil {
			err = errors.Join(v.Get(&view), v.Get(&sub))
		}
		want := Subscription{Cycle: 4, Status: SubActive, Charged: 4_000}
		if err != nil || view.Subscription != want || view.Terms.Merchant != "streamer" || view.Terms.Cycle != 2*day || sub != want {
			t.Errorf("query = %+v and %+v, %v; want %+v with the terms", view, sub, err, want)
		}
	}, day)

	env.ExecuteWorkflow(SubscriptionWorkflow, terms, Subscription{Cycle: 3, Status: SubActive, Charged: 3_000})
}
