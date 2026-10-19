package main

import (
	"fmt"
	"maps"
	"testing"

	"go.temporal.io/sdk/testsuite"
)

// A card's accounts after a $400 hold, a $250 capture and 250 points.
func healthyCard(id string) map[string]int64 {
	return map[string]int64{
		"card:" + id + ":holds":       0,
		"card:" + id + ":posted":      25_000,
		"card:" + id + ":open-to-buy": -25_000,
		"card:" + id + ":points":      250,
		"partner:shop:points-issued":  -250,
	}
}

func with(base map[string]int64, changes map[string]int64) map[string]int64 {
	out := maps.Clone(base)
	if out == nil {
		out = map[string]int64{}
	}
	for k, v := range changes {
		out[k] += v
	}
	return out
}

// Every invariant the ledger must hold, and the finding each break produces.
func TestAuditLedger(t *testing.T) {
	tests := []struct {
		name     string
		accounts map[string]int64
		want     []Finding
	}{
		{"empty ledger", nil, nil},
		{"healthy card", healthyCard("c1"), nil},
		{"two healthy cards and a paid statement", with(with(healthyCard("c1"), healthyCard("c2")), map[string]int64{
			"partner:shop:points-issued": 0, "card:c2:paid:s1": 5_000, "card:c2:payments": -5_000,
		}), nil},
		{"card money off by a cent", with(healthyCard("c1"), map[string]int64{"card:c1:posted": 1, "suspense": -1}),
			[]Finding{{CheckCardMoney, "c1", "holds + posted + open-to-buy = 1"}}},
		{"negative holds", with(healthyCard("c1"), map[string]int64{"card:c1:holds": -500, "card:c1:open-to-buy": 500}),
			[]Finding{{CheckHolds, "c1", "holds = -500"}}},
		{"negative points", with(healthyCard("c1"), map[string]int64{"card:c1:points": -300, "partner:shop:points-issued": 300}),
			[]Finding{{CheckPoints, "c1", "points + redeemed = -50"}}},
		// 250 earned, all redeemed, then a refund claws back 100: below zero, but
		// never below what was earned (SPEC M21).
		{"a clawback after a redemption", with(healthyCard("c1"), map[string]int64{
			"card:c1:points": -350, "card:c1:points-redeemed": 250, "partner:shop:points-issued": 100,
		}), nil},
		{"clawed back past what was earned", with(healthyCard("c1"), map[string]int64{
			"card:c1:points": -550, "card:c1:points-redeemed": 250, "partner:shop:points-issued": 300,
		}), []Finding{{CheckPoints, "c1", "points + redeemed = -50"}}},
		{"points that don't net to zero", with(healthyCard("c1"), map[string]int64{"card:c1:points": 10, "suspense": -10}),
			[]Finding{{CheckPointsNet, "", "card points + redeemed + partner points-issued = 10"}}},
		{"a redemption that lost its other half", with(healthyCard("c1"), map[string]int64{"card:c1:points": -250, "suspense": 250}),
			[]Finding{{CheckPointsNet, "", "card points + redeemed + partner points-issued = -250"}}},
		{"payment memo off", with(healthyCard("c1"), map[string]int64{"card:c1:paid:s1": 5_000, "card:c1:payments": -4_000, "suspense": -1_000}),
			[]Finding{{CheckPaymentMemos, "c1", "paid + payments = 1000"}}},
		{"ledger doesn't sum to zero", with(healthyCard("c1"), map[string]int64{"suspense": 7}),
			[]Finding{{CheckLedgerTotal, "", "all accounts = 7"}}},
		{"only the broken card is named", with(with(healthyCard("c1"), healthyCard("c2")), map[string]int64{
			"partner:shop:points-issued": 0, "card:c2:holds": 3, "suspense": -3,
		}), []Finding{{CheckCardMoney, "c2", "holds + posted + open-to-buy = 3"}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := auditLedger(tt.accounts); fmt.Sprint(got) != fmt.Sprint(tt.want) {
				t.Errorf("findings = %v\n                  want %v", got, tt.want)
			}
		})
	}
}

// Without a Temporal client the workflow checks are skipped, so the workflow
// reports the ledger's findings alone.
func TestAuditWorkflow(t *testing.T) {
	var s testsuite.WorkflowTestSuite
	env := s.NewTestWorkflowEnvironment()
	l := NewMemLedger()
	if err := l.Post(t.Context(), "broken", Entry{"card:c1:holds", -100}, Entry{"card:c1:open-to-buy", 100}); err != nil {
		t.Fatal(err)
	}
	env.RegisterActivity(&Activities{Ledger: l})

	env.ExecuteWorkflow(AuditWorkflow, AuditOptions{})

	var report AuditReport
	if err := env.GetWorkflowResult(&report); err != nil {
		t.Fatal(err)
	}
	if want := []Finding{{CheckHolds, "c1", "holds = -100"}}; fmt.Sprint(report.Findings) != fmt.Sprint(want) {
		t.Errorf("findings = %v, want %v", report.Findings, want)
	}
}
