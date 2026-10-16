package main

// Load test: auths per second and authorize-decision latency against a real
// Temporal dev server and MySQL, with the ledger's invariants checked after.
// Opt-in: CAS_LOAD=1 and CAS_MYSQL_DSN. Not part of CI, which runs on shared
// runners whose timing would make the numbers noise.

import (
	"context"
	"fmt"
	"os"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
)

func TestLoad(t *testing.T) {
	if os.Getenv("CAS_LOAD") == "" || os.Getenv("CAS_MYSQL_DSN") == "" {
		t.Skip("set CAS_LOAD=1 and CAS_MYSQL_DSN to run the load test")
	}
	ctx := context.Background()
	srv, err := testsuite.StartDevServer(ctx, testsuite.DevServerOptions{LogLevel: "error"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = srv.Stop() })
	c := srv.Client()
	l := open(t, freshDB(t)) // its own database, so the audit sees only this load

	w := worker.New(c, TaskQueue, worker.Options{})
	w.RegisterWorkflow(AuthorizeWorkflow)
	w.RegisterActivity(&Activities{Ledger: l, Config: l})
	if err := w.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(w.Stop)

	const amount = 1_000 // $10 per auth
	scenarios := []struct {
		name         string
		auths, cards int
		limit        int64 // per card
		wantApproved int64
	}{
		{"spread: 400 auths over 100 cards", 400, 100, 1_000_000, 400},
		// More than 5 attempts per card in 60s decline, however many race.
		{"hot card: 400 at once, velocity admits 5", 400, 1, 1_000_000, VelocityMax},
		// 5 concurrent attempts per card (within velocity) against limits that
		// cover 3: the ledger's row locks admit exactly 3 on each.
		{"limit contention: 100 auths over 20 cards, limits cover 3 each", 100, 20, 3 * amount, 60},
	}
	const concurrency = 50
	for _, sc := range scenarios {
		t.Run(sc.name, func(t *testing.T) {
			prefix := fmt.Sprintf("load-%d-%d", runID%1_000_000, len(sc.name))
			for i := range sc.cards {
				if err := l.SetCreditLimit(ctx, fmt.Sprintf("%s-c%d", prefix, i), sc.limit); err != nil {
					t.Fatal(err)
				}
			}
			var approved atomic.Int64
			latencies := make([]time.Duration, sc.auths)
			jobs := make(chan int)
			var wg sync.WaitGroup
			start := time.Now()
			for range concurrency {
				wg.Go(func() {
					for i := range jobs {
						card := fmt.Sprintf("%s-c%d", prefix, i%sc.cards)
						id := fmt.Sprintf("%s-auth%d", prefix, i)
						dec, decided, err := authorizeAndSettle(ctx, c, id, AuthRequest{CardID: card, Amount: amount, Merchant: "shop", RefundWindow: time.Millisecond})
						latencies[i] = decided
						if err != nil {
							t.Error(err)
							continue
						}
						if dec.Approved {
							approved.Add(1)
						}
					}
				})
			}
			for i := range sc.auths {
				jobs <- i
			}
			close(jobs)
			wg.Wait()
			elapsed := time.Since(start)

			slices.Sort(latencies)
			pct := func(p float64) time.Duration {
				return latencies[int(p*float64(len(latencies)-1))].Round(time.Millisecond)
			}
			t.Logf("%d auths (approved ones captured too) in %v: %.0f auths/s; decision p50 %v, p99 %v, max %v; %d approved",
				sc.auths, elapsed.Round(time.Millisecond), float64(sc.auths)/elapsed.Seconds(), pct(0.5), pct(0.99), pct(1), approved.Load())

			// Correctness under load.
			if approved.Load() != sc.wantApproved {
				t.Errorf("approved %d, want exactly %d", approved.Load(), sc.wantApproved)
			}
			waitForAuths(t, c, prefix)
			for i := range sc.cards {
				card := fmt.Sprintf("%s-c%d", prefix, i)
				if h := balance(t, l, holdsAccount(card)); h != 0 {
					t.Errorf("%s holds = %d after every auth settled, want 0", card, h)
				}
				if p := balance(t, l, postedAccount(card)); p > sc.limit {
					t.Errorf("%s posted %d over its %d limit", card, p, sc.limit)
				}
			}
			if posted := sumPosted(t, l, prefix, sc.cards); posted != sc.wantApproved*amount {
				t.Errorf("posted %d across the cards, want %d (one capture per approval)", posted, sc.wantApproved*amount)
			}
			accounts, err := l.Accounts(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if findings := auditLedger(accounts); len(findings) > 0 {
				t.Errorf("audit after load: %v", findings)
			}
		})
	}
}

// authorizeAndSettle asks for a decision and, if approved, captures in full.
// decided is how long the decision took, the latency a network waits on.
func authorizeAndSettle(ctx context.Context, c client.Client, id string, req AuthRequest) (dec Decision, decided time.Duration, err error) {
	began := time.Now()
	h, err := c.UpdateWithStartWorkflow(ctx, client.UpdateWithStartWorkflowOptions{
		StartWorkflowOperation: c.NewWithStartWorkflowOperation(client.StartWorkflowOptions{
			ID: id, TaskQueue: TaskQueue, WorkflowIDConflictPolicy: enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING,
		}, AuthorizeWorkflow, req),
		UpdateOptions: client.UpdateWorkflowOptions{UpdateName: AuthorizeUpdate, WaitForStage: client.WorkflowUpdateStageCompleted},
	})
	if err != nil {
		return dec, 0, err
	}
	err = h.Get(ctx, &dec)
	decided = time.Since(began)
	if err != nil || !dec.Approved {
		return dec, decided, err
	}
	ch, err := c.UpdateWorkflow(ctx, client.UpdateWorkflowOptions{
		WorkflowID: id, UpdateName: CaptureSignal, Args: []any{Capture{Amount: req.Amount}}, WaitForStage: client.WorkflowUpdateStageCompleted,
	})
	if err != nil {
		return dec, decided, err
	}
	return dec, decided, ch.Get(ctx, nil)
}

// waitForAuths waits until none of this scenario's auths is still running.
func waitForAuths(t *testing.T, c client.Client, prefix string) {
	t.Helper()
	query := fmt.Sprintf("WorkflowType = 'AuthorizeWorkflow' AND ExecutionStatus = 'Running' AND WorkflowId STARTS_WITH '%s'", prefix)
	for deadline := time.Now().Add(2 * time.Minute); time.Now().Before(deadline); time.Sleep(200 * time.Millisecond) {
		n, err := c.CountWorkflow(context.Background(), &workflowservice.CountWorkflowExecutionsRequest{Query: query})
		if err == nil && n.GetCount() == 0 {
			return
		}
	}
	t.Fatalf("auths under %s still running after 2 minutes", prefix)
}

func sumPosted(t *testing.T, l Ledger, prefix string, cards int) int64 {
	t.Helper()
	var sum int64
	for i := range cards {
		sum += balance(t, l, postedAccount(fmt.Sprintf("%s-c%d", prefix, i)))
	}
	return sum
}
