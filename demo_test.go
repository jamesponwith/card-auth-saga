package main

// The README's demo: one card's story told by the real workflow in the test
// environment (time skips, so days pass in milliseconds), with every ledger
// posting printed as it happens. docs/demo.tape renders it to docs/demo.gif.
// Opt-in: CAS_DEMO=1; CAS_DEMO_PACE paces each line for the recording.

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"go.temporal.io/sdk/client"
	sdklog "go.temporal.io/sdk/log"
	"go.temporal.io/sdk/testsuite"
)

const bold, dim, cyan, green, yellow, red, reset = "\033[1m", "\033[2m", "\033[36m", "\033[32m", "\033[33m", "\033[31m", "\033[0m"

func TestDemo(t *testing.T) {
	if os.Getenv("CAS_DEMO") == "" {
		t.Skip("set CAS_DEMO=1 to print the demo")
	}
	pace := 0 * time.Millisecond
	if os.Getenv("CAS_DEMO_PACE") != "" {
		pace = 450 * time.Millisecond
	}
	say := func(format string, args ...any) {
		fmt.Printf(format+"\n", args...)
		time.Sleep(pace)
	}
	l := &narratedLedger{Ledger: NewMemLedger(), say: say, crashOn: "capture:20000", seen: map[string]bool{}}
	cfg := StaticConfig{
		Limits: map[string]int64{"card-1": 100_000, "card-2": 100_000},
		Rates:  map[string]int64{"booking": 5},
	}

	say("%scard-auth-saga%s %s· Temporal workflows · double-entry ledger%s\n", bold, reset, dim, reset)
	var s testsuite.WorkflowTestSuite
	s.SetLogger(sdklog.NewStructuredLogger(slog.New(slog.NewTextHandler(io.Discard, nil)))) // the postings tell the story
	env := s.NewTestWorkflowEnvironment()
	env.RegisterActivity(&Activities{Ledger: l, Config: cfg})
	start := env.Now()
	step := func(at time.Duration, what string, name string, arg any) {
		env.RegisterDelayedCallback(func() {
			say("%s▶ %-4s %s%s", bold, "+"+compact(env.Now().Sub(start)), what, reset)
			env.UpdateWorkflow(name, what, &testsuite.TestUpdateCallback{
				OnReject:   func(err error) { say("  %s✗ rejected: %v%s", red, err, reset) },
				OnComplete: func(any, error) {},
			}, arg)
		}, at)
	}
	env.RegisterDelayedCallback(func() {
		say("%s▶ authorize%s card-1 · $400 at booking %s(limit $1,000 · 5 pts/$)%s", bold, reset, dim, reset)
		env.UpdateWorkflow(AuthorizeUpdate, "auth", &testsuite.TestUpdateCallback{
			OnComplete: func(r any, err error) {
				if d, ok := r.(Decision); ok && d.Approved {
					say("  %s✓ approved%s %sin the same round trip that started the workflow%s", green, reset, dim, reset)
				}
			},
		})
	}, 0)
	step(hr, "hotel adds $100", IncrementUpdate, Increment{ID: "i1", Amount: 10_000})
	step(day, "first shipment: capture $200", CaptureSignal, Capture{Amount: 20_000, Partial: true})
	step(2*day, "last shipment: capture $250", CaptureSignal, Capture{Amount: 25_000})
	step(3*day, "refund r1 $50", RefundSignal, Refund{ID: "r1", Amount: 5_000})
	step(4*day, "dispute d1 $100", DisputeUpdate, Dispute{ID: "d1", Amount: 10_000})
	step(5*day, "merchant contests d1", MerchantResponseUpdate, MerchantResponse{DisputeID: "d1"})
	step(6*day, "analyst rules for the cardholder", ResolveDisputeUpdate, Ruling{DisputeID: "d1", CardholderWins: true})

	env.ExecuteWorkflow(AuthorizeWorkflow, AuthRequest{CardID: "card-1", Amount: 40_000, Merchant: "booking", RefundWindow: 10 * day})
	var res AuthResult
	if err := env.GetWorkflowResult(&res); err != nil {
		t.Fatal(err)
	}
	say("%s✓ %s%s · $%d captured · $%d refunded · $%d charged back · %d pts kept", green, res.Status, reset,
		res.Captured/100, res.Refunded/100, res.ChargedBack/100, res.Points)

	// Card testing: six auths in a second on another card.
	say("%s▶ six $1 auths in a second on card-2%s", bold, reset)
	var reasons []string
	for i := range 6 {
		env := s.NewTestWorkflowEnvironment()
		env.RegisterActivity(&Activities{Ledger: l.Ledger, Config: cfg}) // unnarrated
		env.SetStartWorkflowOptions(client.StartWorkflowOptions{ID: fmt.Sprintf("burst-%d", i)})
		env.ExecuteWorkflow(AuthorizeWorkflow, AuthRequest{CardID: "card-2", Amount: 100, Merchant: "shop", HoldFor: time.Minute})
		var r AuthResult
		if err := env.GetWorkflowResult(&r); err != nil {
			t.Fatal(err)
		}
		reasons = append(reasons, cmp.Or(r.Reason, "approved"))
	}
	say("  %s · %s6th declined: %s%s", strings.Join(reasons[:5], " · "), red, reasons[5], reset)

	accounts, err := l.Accounts(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if findings := auditLedger(accounts); len(findings) > 0 {
		t.Fatalf("audit: %v", findings)
	}
	say("%s✓ audit: ledger Σ = 0 · 0 findings%s", green, reset)
}

// compact is a duration as the demo prints it: 1h, 2d.
func compact(d time.Duration) string {
	if d >= day {
		return fmt.Sprintf("%dd", d/day)
	}
	return fmt.Sprintf("%dh", d/time.Hour)
}

// narratedLedger prints each new posting, and on its first write of the
// crashOn step, applies it and then fails, as a crash after the commit would.
type narratedLedger struct {
	Ledger
	say     func(string, ...any)
	crashOn string

	mu   sync.Mutex
	seen map[string]bool
}

func (n *narratedLedger) Post(ctx context.Context, key string, entries ...Entry) error {
	_, err := n.PostWithin(ctx, key, nil, 0, entries...)
	return err
}

func (n *narratedLedger) PostWithin(ctx context.Context, key string, capped []string, limit int64, entries ...Entry) (bool, error) {
	ok, err := n.Ledger.PostWithin(ctx, key, capped, limit, entries...)
	if err != nil || !ok {
		return ok, err
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	_, step, _ := strings.Cut(key, ":")
	if n.seen[key] {
		n.say("  %s%-15s%s %salready posted · idempotent no-op%s", cyan, label(step), reset, yellow, reset)
		return ok, nil
	}
	n.seen[key] = true
	var parts []string
	for _, e := range entries {
		if e.Amount != 0 {
			parts = append(parts, short(e))
		}
	}
	n.say("  %s%-15s%s %s", cyan, label(step), reset, strings.Join(parts, " · "))
	if step == n.crashOn { // first write only: a retry returns at seen above
		n.say("  %s✗ crash after the write%s %s— Temporal retries the activity%s", red, reset, dim, reset)
		return false, errors.New("injected crash after the write")
	}
	return ok, nil
}

// label names a posting by its step: capture:20000 is a capture, and so on.
func label(step string) string {
	switch {
	case strings.HasPrefix(step, "capture:"):
		return "capture"
	case strings.HasPrefix(step, "hold:"):
		return "increment"
	case strings.HasPrefix(step, "rewards:refund:"):
		return "clawback " + strings.TrimPrefix(step, "rewards:refund:")
	case strings.HasPrefix(step, "rewards:dispute:"):
		return "chargeback " + strings.TrimPrefix(step, "rewards:dispute:")
	case strings.HasPrefix(step, "dispute:") && strings.HasSuffix(step, ":credit"):
		return "credit " + strings.TrimSuffix(strings.TrimPrefix(step, "dispute:"), ":credit")
	}
	return step
}

// short is an entry as the demo prints it: holds $400, points -250 pts.
func short(e Entry) string {
	name := e.Account[strings.LastIndex(e.Account, ":")+1:]
	if strings.HasSuffix(e.Account, "points") || strings.HasSuffix(e.Account, "points-issued") {
		return fmt.Sprintf("%s %+d pts", name, e.Amount)
	}
	if e.Amount < 0 {
		return fmt.Sprintf("%s -$%d", name, -e.Amount/100)
	}
	return fmt.Sprintf("%s $%d", name, e.Amount/100)
}
