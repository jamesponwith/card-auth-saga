package main

// Failure drills: the real worker against a real Temporal dev server, which the
// SDK downloads and starts. Skipped under -short. Each drill checks the ledger
// is exactly right afterwards, then replays the recorded history to prove the
// workflow is deterministic. CAS_UPDATE_GOLDEN=1 saves those histories to
// testdata/, where TestReplayGoldenHistories replays them on every commit.

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/temporalproto"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
)

func TestDrills(t *testing.T) {
	if testing.Short() {
		t.Skip("drills start a Temporal dev server; run without -short")
	}
	ctx := context.Background()
	srv, err := testsuite.StartDevServer(ctx, testsuite.DevServerOptions{LogLevel: "error", ClientOptions: &client.Options{Identity: "drill-client"},
		ExtraArgs: []string{"--search-attribute", "CardID=Keyword"}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = srv.Stop() })
	c := srv.Client()

	t.Run("worker killed mid-hold", func(t *testing.T) {
		d := newDrill(t, c, "kill", nil)
		w1 := d.startWorker()
		run := d.start(AuthRequest{Amount: 40_000, RefundWindow: 2 * time.Second})
		d.waitBalance(holdsAccount(d.card), 40_000)

		w1.Stop() // no worker: the capture waits in the server's history, not in memory
		d.signal(run, CaptureSignal, Capture{Amount: 40_000})
		time.Sleep(500 * time.Millisecond)
		d.startWorker() // a fresh worker replays history and carries on

		d.expect(run, AuthResult{Status: StatusCaptured, Captured: 40_000, Points: 400})
		d.expectBalances(0, 40_000, 400)
	})

	t.Run("duplicate signals", func(t *testing.T) {
		d := newDrill(t, c, "dupe", nil)
		d.startWorker()
		run := d.start(AuthRequest{Amount: 40_000, RefundWindow: 3 * time.Second})
		d.waitBalance(holdsAccount(d.card), 40_000)
		for range 3 {
			d.signal(run, CaptureSignal, Capture{Amount: 40_000})
		}
		d.waitBalance(postedAccount(d.card), 40_000)
		for range 3 {
			d.signal(run, RefundSignal, Refund{ID: "r1", Amount: 15_000})
		}

		d.expect(run, AuthResult{Status: StatusCaptured, Captured: 40_000, Refunded: 15_000, Points: 250})
		d.expectBalances(0, 25_000, 250)
	})

	t.Run("activity times out after writing", func(t *testing.T) {
		old := activityTimeout
		activityTimeout = time.Second
		t.Cleanup(func() { activityTimeout = old })
		slow := &slowFirstLedger{delay: 2 * time.Second, seen: map[string]bool{}} // applies the write, then misses the deadline
		d := newDrill(t, c, "timeout", slow)
		d.startWorker()
		run := d.start(AuthRequest{Amount: 40_000, RefundWindow: time.Second})
		d.waitBalance(holdsAccount(d.card), 40_000)
		d.signal(run, CaptureSignal, Capture{Amount: 30_000})

		d.expect(run, AuthResult{Status: StatusCaptured, Captured: 30_000, Points: 300})
		d.expectBalances(0, 30_000, 300)
		if calls, keys := slow.stats(); calls <= keys {
			t.Errorf("ledger saw %d calls for %d keys; want retries after the timeouts", calls, keys)
		}
	})

	t.Run("update with start", func(t *testing.T) {
		d := newDrill(t, c, "uws", nil)
		d.startWorker()
		req := AuthRequest{Amount: 40_000, RefundWindow: time.Second}
		for range 2 { // the network retries: same auth ID, same answer, one hold
			if !d.authorize(d.card, req).Approved {
				t.Fatal("want approved")
			}
		}
		d.waitBalance(holdsAccount(d.card), 40_000)
		run := d.c.GetWorkflow(context.Background(), d.card, "")
		d.signal(run, CaptureSignal, Capture{Amount: 40_000})

		d.expect(run, AuthResult{Status: StatusCaptured, Captured: 40_000, Points: 400})
		d.expectBalances(0, 40_000, 400)
	})

	t.Run("updates mid-step", func(t *testing.T) {
		// Every first write stalls 1.5s, so the refund lands while settlement
		// rewards are posting, and the 1s window closes while the refund posts.
		slow := &slowFirstLedger{delay: 1500 * time.Millisecond, seen: map[string]bool{}}
		d := newDrill(t, c, "updates", slow)
		d.startWorker()
		if !d.authorize(d.card, AuthRequest{Amount: 40_000, RefundWindow: time.Second}).Approved {
			t.Fatal("want approved")
		}
		if err := d.update(CaptureSignal, "c1", Capture{Amount: 40_000}); err != nil {
			t.Fatal(err)
		}
		var wg sync.WaitGroup
		for range 2 { // a retried request reuses the update ID: one refund
			wg.Go(func() {
				if err := d.update(RefundSignal, "r1", Refund{ID: "r1", Amount: 15_000}); err != nil {
					t.Error(err)
				}
			})
		}
		wg.Wait()
		if err := d.update(RefundSignal, "r1-new-id", Refund{ID: "r1", Amount: 15_000}); err == nil {
			t.Error("want a used refund ID rejected")
		}

		d.expect(d.c.GetWorkflow(context.Background(), d.card, ""), AuthResult{Status: StatusCaptured, Captured: 40_000, Refunded: 15_000, Points: 250})
		d.expectBalances(0, 25_000, 250)
	})

	t.Run("increments and split capture", func(t *testing.T) {
		d := newDrill(t, c, "split", nil)
		d.startWorker()
		if !d.authorize(d.card, AuthRequest{Amount: 40_000, RefundWindow: time.Second}).Approved {
			t.Fatal("want approved")
		}
		for _, inc := range []struct {
			id     string
			amount int64
			want   bool
		}{{"i1", 20_000, true}, {"i2", 50_000, false}} { // 60,000 held; 110,000 is over the limit
			var ok bool
			if err := d.updateResult(IncrementUpdate, inc.id, Increment{ID: inc.id, Amount: inc.amount}, &ok); err != nil || ok != inc.want {
				t.Fatalf("increment %s = %v, %v; want %v", inc.id, ok, err, inc.want)
			}
		}
		if err := d.update(CaptureSignal, "c1", Capture{Amount: 30_000, Partial: true}); err != nil {
			t.Fatal(err)
		}
		if err := d.update(CaptureSignal, "c2", Capture{Amount: 25_000}); err != nil { // releases the last 5,000
			t.Fatal(err)
		}

		d.expect(d.c.GetWorkflow(context.Background(), d.card, ""), AuthResult{Status: StatusCaptured, Captured: 55_000, Points: 550})
		d.expectBalances(0, 55_000, 550)
	})

	t.Run("statement cycles", func(t *testing.T) {
		d := newDrill(t, c, "stmt", nil)
		d.startWorker()
		ctx := context.Background()
		if err := d.l.Post(ctx, d.card+":seed", Entry{postedAccount(d.card), 50_000}, Entry{openToBuyAccount(d.card), -50_000}); err != nil {
			t.Fatal(err)
		}
		id := StatementWorkflowID(d.card)
		// 10 "days" of 100ms per 1s cycle; payment due on day 4.
		if _, err := d.c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: id, TaskQueue: TaskQueue}, StatementWorkflow,
			StatementTerms{CardID: d.card, Cycle: time.Second, Grace: 400 * time.Millisecond, Day: 100 * time.Millisecond}, Statement{}); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = d.c.TerminateWorkflow(ctx, id, "", "drill done") })

		var wg sync.WaitGroup
		for range 2 { // in the first cycle; a resent request reuses the update ID
			wg.Go(func() {
				h, err := d.c.UpdateWorkflow(ctx, client.UpdateWorkflowOptions{WorkflowID: id, UpdateID: "p1", UpdateName: PayUpdate,
					Args: []any{Payment{ID: "p1", Amount: 1_000}}, WaitForStage: client.WorkflowUpdateStageCompleted})
				if err == nil {
					err = h.Get(ctx, nil)
				}
				if err != nil {
					t.Error(err)
				}
			})
		}
		wg.Wait()
		if got := d.waitStatement(id, 1); got.Balance != 49_000 {
			t.Fatalf("statement 1 = %+v, want a 49,000 balance (one payment)", got)
		}

		// Nothing paid on statement 1: the fee lands on day 4, before its sample.
		// 3 days at 49,000 + 7 at 51,500 = 507,500 → 347.46 interest.
		want := Statement{N: 2, Balance: 51_847, MinDue: 2_500, Fees: 2_500, Interest: 347}
		if got := d.waitStatement(id, 2); got != want {
			t.Errorf("statement = %+v, want %+v", got, want)
		}
		if got := balance(t, d.l, postedAccount(d.card)); got != want.Balance {
			t.Errorf("posted = %d, want %d", got, want.Balance)
		}
		d.expectBalances(0, want.Balance, 0)
		// The run that closed statement 2 is complete: replay it.
		cur := d.c.GetWorkflow(ctx, id, "")
		d.replay(d.c.GetWorkflow(ctx, id, d.previousRun(id, cur.GetRunID())))
	})

	t.Run("card lifecycle", func(t *testing.T) {
		dsn := os.Getenv("CAS_MYSQL_DSN")
		if dsn == "" {
			t.Skip("set-limit keeps cards in MySQL; set CAS_MYSQL_DSN")
		}
		ctx := context.Background()
		card := fmt.Sprintf("drill-life-%d", runID)
		env := envOf(map[string]string{"CAS_MYSQL_DSN": dsn, "TEMPORAL_ADDRESS": srv.FrontendHostPort()})
		t.Cleanup(func() { _ = c.TerminateWorkflow(ctx, StatementWorkflowID(card), "", "drill done") })

		var runs []string
		for _, limit := range []string{"100000", "150000"} { // create, then raise: one statement cycle throughout
			if err := run(ctx, []string{"set-limit", card, limit}, env); err != nil {
				t.Fatal(err)
			}
			desc, err := c.DescribeWorkflowExecution(ctx, StatementWorkflowID(card), "")
			if err != nil {
				t.Fatal(err)
			}
			if st := desc.GetWorkflowExecutionInfo().GetStatus(); st != enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING {
				t.Fatalf("statement workflow %v, want running", st)
			}
			runs = append(runs, desc.GetWorkflowExecutionInfo().GetExecution().GetRunId())
		}
		if runs[0] != runs[1] {
			t.Errorf("set-limit started a second statement run: %v", runs)
		}
	})

	t.Run("disputes", func(t *testing.T) {
		d := newDrill(t, c, "dispute", nil)
		d.startWorker()
		// Seconds-long deadlines; refunds and disputes open for 6s after settlement.
		req := AuthRequest{Amount: 40_000, RefundWindow: 6 * time.Second, MerchantResponse: 2 * time.Second, DisputeReview: 2 * time.Second}
		if !d.authorize(d.card, req).Approved {
			t.Fatal("want approved")
		}
		steps := []struct {
			name, id string
			arg      any
		}{
			{CaptureSignal, "c1", Capture{Amount: 40_000}},
			{DisputeUpdate, "d1", Dispute{ID: "d1", Amount: 15_000}}, // contested, cardholder wins
			{MerchantResponseUpdate, "d1-response", MerchantResponse{DisputeID: "d1", Accept: false}},
			{ResolveDisputeUpdate, "d1-ruling", Ruling{DisputeID: "d1", CardholderWins: true}},
			{DisputeUpdate, "d2", Dispute{ID: "d2", Amount: 10_000}}, // the merchant never answers
			{DisputeUpdate, "d3", Dispute{ID: "d3", Amount: 5_000}},  // contested, merchant wins
			{MerchantResponseUpdate, "d3-response", MerchantResponse{DisputeID: "d3", Accept: false}},
			{ResolveDisputeUpdate, "d3-ruling", Ruling{DisputeID: "d3", CardholderWins: false}},
		}
		for _, st := range steps {
			if err := d.update(st.name, st.id, st.arg); err != nil {
				t.Fatalf("%s %s: %v", st.name, st.id, err)
			}
		}

		// d1 and d2 charged back (25,000); d3 rebilled; points on the 15,000 kept.
		d.expect(d.c.GetWorkflow(context.Background(), d.card, ""), AuthResult{Status: StatusCaptured, Captured: 40_000, ChargedBack: 25_000, Points: 150})
		d.expectBalances(0, 15_000, 150)
	})

	t.Run("health names a MySQL outage", func(t *testing.T) {
		dbDown := func(context.Context) error { return errors.New("connection refused") }
		if got := workerHealth(c, dbDown)(); got != "degraded: mysql: connection refused" {
			t.Errorf("health with MySQL down = %q, want degraded: mysql", got)
		}
	})

	t.Run("worker process", func(t *testing.T) {
		ln, err := net.Listen("tcp", "127.0.0.1:0") // a free port for the ops endpoint
		if err != nil {
			t.Fatal(err)
		}
		addr := ln.Addr().String()
		dln, err := net.Listen("tcp", "127.0.0.1:0") // and one for the metrics, kept off the public port
		if err != nil {
			t.Fatal(err)
		}
		debugAddr := dln.Addr().String()
		_ = dln.Close()
		env := envOf(map[string]string{"TEMPORAL_ADDRESS": srv.FrontendHostPort(), "CAS_HTTP_ADDR": addr, "CAS_DEBUG_ADDR": debugAddr})
		if err := run(context.Background(), nil, env); err == nil || !strings.Contains(err.Error(), "ops endpoint") {
			t.Errorf("run with the ops port taken = %v, want it to refuse to start", err)
		}
		_ = ln.Close()

		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- run(ctx, nil, env) }()
		var health map[string]any
		deadline := time.Now().Add(30 * time.Second)
		for time.Now().Before(deadline) && health["status"] != "ok" {
			if res, err := http.Get("http://" + addr + "/healthz"); err == nil {
				_ = json.NewDecoder(res.Body).Decode(&health)
				_ = res.Body.Close()
			}
			time.Sleep(100 * time.Millisecond)
		}
		if health["status"] != "ok" || health["version"] != Version {
			t.Errorf("/healthz = %v, want ok and version %s", health, Version)
		}
		// The metrics aren't on the public port; the worker's Temporal client
		// reports into them on the private one.
		if res, err := http.Get("http://" + addr + "/debug/vars"); err != nil || res.StatusCode != http.StatusNotFound {
			t.Errorf("/debug/vars on the public port = %v, %v; want 404", res, err)
		} else {
			_ = res.Body.Close()
		}
		var vars struct{ Temporal map[string]any }
		if res, err := http.Get("http://" + debugAddr + "/debug/vars"); err != nil {
			t.Error(err)
		} else {
			_ = json.NewDecoder(res.Body).Decode(&vars)
			_ = res.Body.Close()
		}
		polls := 0
		for k := range vars.Temporal {
			if strings.HasPrefix(k, "temporal_long_request") || strings.HasPrefix(k, "temporal_request") {
				polls++
			}
		}
		if polls == 0 {
			t.Errorf("/debug/vars has no Temporal client metrics: %v", vars.Temporal)
		}
		cancel() // as SIGTERM would
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("run after shutdown: %v", err)
			}
		case <-time.After(30 * time.Second):
			t.Fatal("worker did not stop on cancel")
		}
		if _, err := http.Get("http://" + addr + "/healthz"); err == nil {
			t.Error("ops endpoint still serving after shutdown")
		}
	})

	t.Run("audit", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute) // fail, don't hang
		defer cancel()
		d := newDrill(t, c, "audit", nil)
		d.acts.Temporal = c
		w := d.startWorker()
		held := d.start(AuthRequest{Amount: 1_000}) // a live hold: "stuck" once the threshold is tiny
		d.waitBalance(holdsAccount(d.card), 1_000)
		bad, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: d.card + "-bad", TaskQueue: TaskQueue},
			AuthorizeWorkflow, AuthRequest{CardID: d.card, Amount: 0}) // fails: non-positive amount
		if err != nil {
			t.Fatal(err)
		}
		_ = bad.Get(ctx, nil)

		has := func(findings []Finding, check, subject string) bool {
			return slices.ContainsFunc(findings, func(f Finding) bool { return f.Check == check && f.Subject == subject })
		}
		auditRun, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: d.card + "-audit", TaskQueue: TaskQueue},
			AuditWorkflow, AuditOptions{StuckAuth: time.Millisecond})
		if err != nil {
			t.Fatal(err)
		}
		var report AuditReport
		if err := auditRun.Get(ctx, &report); err != nil {
			t.Fatal(err)
		}
		if !has(report.Findings, CheckStuckAuth, held.GetID()) || !has(report.Findings, CheckFailedWorkflow, bad.GetID()) {
			t.Errorf("findings %v, want the held auth stuck and %s failed", report.Findings, bad.GetID())
		}
		workflowChecks := []string{CheckStuckAuth, CheckStuckStatement, CheckFailedWorkflow}
		for _, f := range report.Findings {
			if f.Subject == d.card && !slices.Contains(workflowChecks, f.Check) {
				t.Errorf("ledger finding on a healthy card: %v", f)
			}
		}

		// Orphaned holds: money held on a card with no auth running on it.
		oldRecheck := orphanRecheck
		orphanRecheck = 100 * time.Millisecond
		t.Cleanup(func() { orphanRecheck = oldRecheck })
		orphan := d.card + "-orphan"
		if err := d.l.Post(ctx, orphan+":stray-hold", Entry{holdsAccount(orphan), 700}, Entry{openToBuyAccount(orphan), -700}); err != nil {
			t.Fatal(err)
		}
		orphans, err := d.acts.orphanedHolds(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if !has(orphans, CheckOrphanedHold, orphan) || has(orphans, CheckOrphanedHold, d.card) {
			t.Errorf("orphaned holds = %v, want %s and not %s (its auth is running)", orphans, orphan, d.card)
		}
		// A closed card's statements wait for its last running auth.
		closed := &Activities{Ledger: NewMemLedger(), Config: StaticConfig{Statuses: map[string]string{d.card: CardClosed}}, Temporal: c}
		if done, err := closed.CardDone(ctx, d.card); err != nil || done {
			t.Errorf("CardDone with an auth running = %v, %v; want false", done, err)
		}

		// An untagged auth (from before CardID): it can't be attributed, so the
		// orphan check stands down and closed cards keep their statements.
		untagged, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: d.card + "-untagged", TaskQueue: "no-worker-polls-this"},
			AuthorizeWorkflow, AuthRequest{CardID: orphan, Amount: 1})
		if err != nil {
			t.Fatal(err)
		}
		for n := int64(0); n == 0; time.Sleep(100 * time.Millisecond) {
			if n, err = d.acts.untaggedAuths(ctx); err != nil {
				t.Fatal(err)
			}
		}
		if orphans, err := d.acts.orphanedHolds(ctx); err != nil || len(orphans) > 0 {
			t.Errorf("orphaned holds with an untagged auth running = %v, %v; want none reported", orphans, err)
		}
		_ = c.TerminateWorkflow(ctx, untagged.GetID(), "", "drill done")

		// The audit command reports the failure and exits non-zero.
		env := envOf(map[string]string{"TEMPORAL_ADDRESS": srv.FrontendHostPort()})
		if err := run(ctx, []string{"audit"}, env); err == nil || !strings.Contains(err.Error(), bad.GetID()) {
			t.Errorf("audit command = %v, want the failed workflow named", err)
		}

		// The schedule: created once, and its latest run drives /healthz.
		for range 2 {
			if err := ensureAuditSchedule(ctx, c); err != nil {
				t.Fatalf("ensureAuditSchedule: %v", err)
			}
		}
		h := c.ScheduleClient().GetHandle(ctx, AuditScheduleID)
		t.Cleanup(func() { // the schedule, and any audit it left running
			bg := context.Background()
			if desc, err := h.Describe(bg); err == nil {
				for _, a := range desc.Info.RecentActions {
					if r := a.StartWorkflowResult; r != nil {
						_ = c.TerminateWorkflow(bg, r.WorkflowID, r.FirstExecutionRunID, "drill done")
					}
				}
			}
			_ = h.Delete(bg)
		})
		if got := workerHealth(c, nil)(); got != "ok" {
			t.Errorf("health before any scheduled audit = %q, want ok", got)
		}
		if err := h.Trigger(ctx, client.ScheduleTriggerOptions{}); err != nil {
			t.Fatal(err)
		}
		deadline := time.Now().Add(30 * time.Second)
		health := ""
		for time.Now().Before(deadline) && !strings.HasPrefix(health, "degraded: audit: ") {
			health = workerHealth(c, nil)()
			time.Sleep(200 * time.Millisecond)
		}
		if !strings.HasPrefix(health, "degraded: audit: ") {
			t.Errorf("health after an audit with findings = %q, want degraded: audit", health)
		}
		// While the next audit is still running (here: no worker to run it),
		// the last finished one stands.
		w.Stop()
		if err := h.Trigger(ctx, client.ScheduleTriggerOptions{}); err != nil {
			t.Fatal(err)
		}
		for desc, err := h.Describe(ctx); err == nil && len(desc.Info.RecentActions) < 2; desc, err = h.Describe(ctx) {
			time.Sleep(100 * time.Millisecond)
		}
		if got := workerHealth(c, nil)(); !strings.HasPrefix(got, "degraded: audit: ") {
			t.Errorf("health while the next audit runs = %q, want the last audit's findings", got)
		}
		_ = c.TerminateWorkflow(ctx, held.GetID(), "", "drill done")
		var done bool
		for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline) && !done; time.Sleep(100 * time.Millisecond) {
			done, err = closed.CardDone(ctx, d.card) // visibility catches up with the termination
		}
		if !done || err != nil {
			t.Errorf("CardDone after the last auth ended = %v, %v; want true", done, err)
		}
	})

	t.Run("card replacement", func(t *testing.T) {
		dsn := os.Getenv("CAS_MYSQL_DSN")
		if dsn == "" {
			t.Skip("card numbers live in MySQL; set CAS_MYSQL_DSN")
		}
		d := newDrill(t, c, "replace", nil)
		d.startWorker()
		ctx := context.Background()
		env := envOf(map[string]string{"CAS_MYSQL_DSN": dsn, "TEMPORAL_ADDRESS": srv.FrontendHostPort()})
		old, replacement := d.card, d.card+"-new"
		ask := func(id, number string, amount int64) Decision { // what the network sends: the number on the card
			return d.authorize(id, AuthRequest{CardID: number, Amount: amount, RefundWindow: time.Second})
		}
		capture := func(id string, amount int64) {
			t.Helper()
			if err := d.updateOn(id, CaptureSignal, "", Capture{Amount: amount}, nil); err != nil {
				t.Fatal(err)
			}
		}

		// Held on the old number when the card is reported lost.
		if !ask(old+"-a1", old, 30_000).Approved {
			t.Fatal("auth on the card before replacement: want approved")
		}
		if err := run(ctx, []string{"replace", old, replacement}, env); err != nil {
			t.Fatal(err)
		}
		if dec := ask(old+"-a2", old, 1_000); dec.Approved || dec.Reason != DeclineReplaced {
			t.Errorf("old number after replacement = %+v, want declined %s", dec, DeclineReplaced)
		}
		if !ask(old+"-a3", replacement, 40_000).Approved { // 30,000 still held: the limit is shared
			t.Fatal("new number: want approved")
		}
		if dec := ask(old+"-a4", replacement, 40_000); dec.Approved || dec.Reason != DeclineOverLimit {
			t.Errorf("new number past the shared limit = %+v, want declined %s", dec, DeclineOverLimit)
		}
		capture(old+"-a1", 30_000)
		capture(old+"-a3", 40_000)
		d.expect(c.GetWorkflow(ctx, old+"-a1", ""), AuthResult{Status: StatusCaptured, Captured: 30_000, Points: 300})
		d.expect(c.GetWorkflow(ctx, old+"-a2", ""), AuthResult{Status: StatusDeclined, Reason: DeclineReplaced})
		d.expect(c.GetWorkflow(ctx, old+"-a3", ""), AuthResult{Status: StatusCaptured, Captured: 40_000, Points: 400})
		d.expectBalances(0, 70_000, 700) // one account: both numbers' spend and points
		if b := balance(t, d.l, postedAccount(replacement)); b != 0 {
			t.Errorf("the new number has ledger accounts of its own: posted %d", b)
		}
		// Every auth is tagged with the account, so the audit and the statement's
		// end-of-life check see them all on one card.
		var tagged int64
		for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline) && tagged != 4; time.Sleep(200 * time.Millisecond) {
			n, err := c.CountWorkflow(ctx, &workflowservice.CountWorkflowExecutionsRequest{Query: fmt.Sprintf("CardID = %q", old)})
			if err == nil {
				tagged = n.GetCount()
			}
		}
		if tagged != 4 {
			t.Errorf("auths tagged %s: %d, want all 4", old, tagged)
		}
	})

	t.Run("subscriptions", func(t *testing.T) {
		dsn := os.Getenv("CAS_MYSQL_DSN")
		if dsn == "" {
			t.Skip("replacement and status changes live in MySQL; set CAS_MYSQL_DSN")
		}
		d := newDrill(t, c, "sub", nil)
		d.acts.Temporal = c // charges authorize through the client, as a network does
		d.startWorker()
		ctx := context.Background()
		env := envOf(map[string]string{"CAS_MYSQL_DSN": dsn, "TEMPORAL_ADDRESS": srv.FrontendHostPort()})
		admin := func(args ...string) {
			t.Helper()
			if err := run(ctx, args, env); err != nil {
				t.Fatal(err)
			}
		}
		// waitFor polls a subscription's query until ok, and returns what it saw last.
		waitFor := func(id string, ok func(Subscription) bool) Subscription {
			t.Helper()
			var sub Subscription
			for deadline := time.Now().Add(20 * time.Second); time.Now().Before(deadline); time.Sleep(100 * time.Millisecond) {
				if v, err := c.QueryWorkflow(ctx, SubscriptionWorkflowID(id), "", SubscriptionQuery); err == nil && v.Get(&sub) == nil && ok(sub) {
					return sub
				}
			}
			t.Fatalf("subscription %s stuck at %+v", id, sub)
			return sub
		}

		// From the CLI: charged at signup, cancelled, and its ID never reused.
		cli := d.card + "-cli"
		admin("set-limit", cli, "100000")                 // a card of its own, so the life below stays under velocity's 5 a minute
		admin("subscribe", cli, cli, cli+"-shop", "1000") // and a merchant: the points checks below are per merchant
		waitFor(cli, func(s Subscription) bool { return s.Charged == 1_000 })
		admin("unsubscribe", cli)
		var res Subscription
		if err := c.GetWorkflow(ctx, SubscriptionWorkflowID(cli), "").Get(ctx, &res); err != nil || res.Status != SubCancelled {
			t.Errorf("after unsubscribe: %+v, %v; want cancelled", res, err)
		}
		if err := run(ctx, []string{"subscribe", cli, cli, cli + "-shop", "1000"}, env); err == nil {
			t.Error("a subscription ID was used twice")
		}

		// A life on a 3s cycle: the card replaced, frozen past a charge and back,
		// then closed. Four charge attempts reach the velocity count; the frozen
		// and closed declines come before it.
		life := d.card + "-life"
		if _, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: SubscriptionWorkflowID(life), TaskQueue: TaskQueue}, SubscriptionWorkflow,
			SubscriptionTerms{ID: life, CardID: d.card, Merchant: d.merchant, Amount: 1_000, Cycle: 3 * time.Second,
				Retries: []time.Duration{time.Second, time.Second, 5 * time.Second}}, Subscription{}); err != nil {
			t.Fatal(err)
		}
		waitFor(life, func(s Subscription) bool { return s.Charged >= 1_000 })
		admin("replace", d.card, d.card+"-new") // the account updater: no re-signup
		at := waitFor(life, func(s Subscription) bool { return s.Charged >= 3_000 }).Charged
		admin("freeze", d.card+"-new")
		waitFor(life, func(s Subscription) bool { return s.Status == SubPastDue && s.LastReason == DeclineFrozen })
		admin("unfreeze", d.card+"-new")
		waitFor(life, func(s Subscription) bool { return s.Status == SubActive && s.Charged > at })
		admin("close", d.card)
		if err := c.GetWorkflow(ctx, SubscriptionWorkflowID(life), "").Get(ctx, &res); err != nil || res.Status != SubEnded || res.LastReason != DeclineClosed {
			t.Fatalf("after close: %+v, %v; want ended, Closed", res, err)
		}

		// Every charge settled on the one account, at 1 point per $1.
		charged := res.Charged
		deadline := time.Now().Add(20 * time.Second) // the last capture's rewards may still be posting
		for time.Now().Before(deadline) && balance(t, d.l, pointsAccount(d.card)) != charged/100 {
			time.Sleep(100 * time.Millisecond)
		}
		d.expectBalances(0, charged, charged/100)
		d.replay(c.GetWorkflow(ctx, SubscriptionWorkflowID(life), ""))

		// A retried charge attempt never charges twice, approved or declined.
		admin("set-limit", d.card+"-retry", "100000")
		for _, tt := range []struct {
			name   string
			before func()
			want   Decision
		}{
			{"approved", func() {}, Decision{Approved: true}},
			{"declined", func() { admin("freeze", d.card+"-retry") }, Decision{Reason: DeclineFrozen}},
		} {
			tt.before()
			id := d.card + "-retry-" + tt.name
			for range 2 {
				if got, err := d.acts.ChargeCard(ctx, id, AuthRequest{CardID: d.card + "-retry", Merchant: d.merchant, Amount: 1_000}); err != nil || got != tt.want {
					t.Errorf("%s: ChargeCard = %+v, %v; want %+v", tt.name, got, err, tt.want)
				}
			}
			runs, err := c.ListWorkflow(ctx, &workflowservice.ListWorkflowExecutionsRequest{Query: fmt.Sprintf("WorkflowId = %q", id)})
			if err != nil || len(runs.GetExecutions()) != 1 {
				t.Errorf("%s: %d runs of %s (%v), want 1", tt.name, len(runs.GetExecutions()), id, err)
			}
		}
		if p := balance(t, d.l, postedAccount(d.card+"-retry")); p != 1_000 {
			t.Errorf("posted after retried charges = %d, want one 1000 charge", p)
		}
	})

	t.Run("update with start declined", func(t *testing.T) {
		d := newDrill(t, c, "uws-decline", nil)
		d.startWorker()
		if d.authorize(d.card, AuthRequest{Amount: 100_001}).Approved {
			t.Fatal("want declined")
		}
		d.expect(d.c.GetWorkflow(context.Background(), d.card, ""), AuthResult{Status: StatusDeclined, Reason: DeclineOverLimit})
		d.expectBalances(0, 0, 0)
	})
}

// drill is one scenario on its own card and merchant, so drills can share a
// ledger (and a MySQL database) without seeing each other's balances.
type drill struct {
	t        *testing.T
	c        client.Client
	l        Ledger
	acts     *Activities
	card     string
	merchant string
}

func newDrill(t *testing.T, c client.Client, name string, wrap *slowFirstLedger) *drill {
	card := fmt.Sprintf("drill-%s-%d", name, runID)
	var l Ledger = NewMemLedger()
	var cfg Config = StaticConfig{Limits: map[string]int64{card: 100_000}}
	if dsn := os.Getenv("CAS_MYSQL_DSN"); dsn != "" { // limits come from the cards table
		ml, err := OpenMySQL(context.Background(), dsn)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = ml.Close() })
		if err := ml.SetCreditLimit(context.Background(), card, 100_000); err != nil {
			t.Fatal(err)
		}
		l, cfg = ml, ml
	}
	if wrap != nil {
		wrap.Ledger = l
		l = wrap
	}
	return &drill{
		t: t, c: c, l: l, card: card, merchant: card + "-shop",
		acts: &Activities{Ledger: l, Config: cfg},
	}
}

func (d *drill) startWorker() worker.Worker {
	w := worker.New(d.c, TaskQueue, worker.Options{Identity: "drill-worker"})
	w.RegisterWorkflow(AuthorizeWorkflow)
	w.RegisterWorkflow(StatementWorkflow)
	w.RegisterWorkflow(AuditWorkflow)
	w.RegisterWorkflow(SubscriptionWorkflow)
	w.RegisterActivity(d.acts)
	if err := w.Start(); err != nil {
		d.t.Fatal(err)
	}
	d.t.Cleanup(w.Stop)
	return w
}

func (d *drill) start(req AuthRequest) client.WorkflowRun {
	req.CardID, req.Merchant = d.card, d.merchant
	run, err := d.c.ExecuteWorkflow(context.Background(),
		client.StartWorkflowOptions{ID: d.card, TaskQueue: TaskQueue}, AuthorizeWorkflow, req)
	if err != nil {
		d.t.Fatal(err)
	}
	return run
}

// authorize starts auth id (or attaches to the running one) and waits for the
// decision. The auth is on the drill's card unless req names another number.
func (d *drill) authorize(id string, req AuthRequest) Decision {
	d.t.Helper()
	req.CardID, req.Merchant = cmp.Or(req.CardID, d.card), d.merchant
	ctx := context.Background()
	start := d.c.NewWithStartWorkflowOperation(client.StartWorkflowOptions{
		ID: id, TaskQueue: TaskQueue, WorkflowIDConflictPolicy: enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING,
	}, AuthorizeWorkflow, req)
	h, err := d.c.UpdateWithStartWorkflow(ctx, client.UpdateWithStartWorkflowOptions{
		StartWorkflowOperation: start,
		UpdateOptions:          client.UpdateWorkflowOptions{UpdateName: AuthorizeUpdate, WaitForStage: client.WorkflowUpdateStageCompleted},
	})
	if err != nil {
		d.t.Fatal(err)
	}
	var dec Decision
	if err := h.Get(ctx, &dec); err != nil {
		d.t.Fatal(err)
	}
	return dec
}

// update sends an update to the drill's workflow and waits for it to finish.
func (d *drill) update(name, id string, arg any) error { return d.updateResult(name, id, arg, nil) }

// updateResult is update, decoding the update's result into result.
func (d *drill) updateResult(name, id string, arg, result any) error {
	return d.updateOn(d.card, name, id, arg, result)
}

// updateOn is updateResult on another of the drill's workflows.
func (d *drill) updateOn(workflowID, name, id string, arg, result any) error {
	h, err := d.c.UpdateWorkflow(context.Background(), client.UpdateWorkflowOptions{
		WorkflowID: workflowID, UpdateID: id, UpdateName: name, Args: []any{arg},
		WaitForStage: client.WorkflowUpdateStageCompleted,
	})
	if err != nil {
		return err
	}
	return h.Get(context.Background(), result)
}

// waitStatement polls the statement query until statement n has closed. Each
// simulated day costs a timer and a few round trips on the dev server.
func (d *drill) waitStatement(id string, n int) Statement {
	d.t.Helper()
	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		var st Statement
		if v, err := d.c.QueryWorkflow(context.Background(), id, "", StatementQuery); err == nil && v.Get(&st) == nil && st.N >= n {
			return st
		}
		time.Sleep(100 * time.Millisecond)
	}
	d.t.Fatalf("statement %d never closed", n)
	return Statement{}
}

// previousRun is the run that continued as new into runID.
func (d *drill) previousRun(id, runID string) string {
	d.t.Helper()
	iter := d.c.GetWorkflowHistory(context.Background(), id, runID, false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	e, err := iter.Next()
	if err != nil {
		d.t.Fatal(err)
	}
	return e.GetWorkflowExecutionStartedEventAttributes().GetContinuedExecutionRunId()
}

func (d *drill) signal(run client.WorkflowRun, name string, payload any) {
	if err := d.c.SignalWorkflow(context.Background(), run.GetID(), run.GetRunID(), name, payload); err != nil {
		d.t.Fatal(err)
	}
}

func (d *drill) waitBalance(account string, want int64) {
	d.t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if balance(d.t, d.l, account) == want {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	d.t.Fatalf("%s never reached %d (now %d)", account, want, balance(d.t, d.l, account))
}

// expect waits for the result, checks it, then replays the history.
func (d *drill) expect(run client.WorkflowRun, want AuthResult) {
	d.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	var got AuthResult
	if err := run.Get(ctx, &got); err != nil {
		d.t.Fatal(err)
	}
	if got != want {
		d.t.Errorf("result = %+v, want %+v", got, want)
	}
	d.replay(run)
}

// expectBalances checks the card's accounts, and that money and points each
// net to zero across every account the drill touched.
func (d *drill) expectBalances(holds, posted, points int64) {
	d.t.Helper()
	b := func(a string) int64 { return balance(d.t, d.l, a) }
	if got := b(holdsAccount(d.card)); got != holds {
		d.t.Errorf("holds = %d, want %d", got, holds)
	}
	if got := b(postedAccount(d.card)); got != posted {
		d.t.Errorf("posted = %d, want %d", got, posted)
	}
	if got := b(pointsAccount(d.card)); got != points {
		d.t.Errorf("points = %d, want %d", got, points)
	}
	if sum := b(holdsAccount(d.card)) + b(postedAccount(d.card)) + b(openToBuyAccount(d.card)); sum != 0 {
		d.t.Errorf("card money nets to %d, want 0", sum)
	}
	if sum := b(pointsAccount(d.card)) + b(partnerPointsAccount(d.merchant)); sum != 0 {
		d.t.Errorf("points net to %d, want 0", sum)
	}
}

func (d *drill) replay(run client.WorkflowRun) {
	d.t.Helper()
	iter := d.c.GetWorkflowHistory(context.Background(), run.GetID(), run.GetRunID(), false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	var h historypb.History
	for iter.HasNext() {
		e, err := iter.Next()
		if err != nil {
			d.t.Fatal(err)
		}
		h.Events = append(h.Events, e)
	}
	r := worker.NewWorkflowReplayer()
	r.RegisterWorkflow(AuthorizeWorkflow)
	r.RegisterWorkflow(StatementWorkflow)
	r.RegisterWorkflow(AuditWorkflow)
	r.RegisterWorkflow(SubscriptionWorkflow)
	if err := r.ReplayWorkflowHistory(nil, &h); err != nil {
		d.t.Errorf("replay: %v", err)
	}
	if os.Getenv("CAS_UPDATE_GOLDEN") != "" {
		bs, err := temporalproto.CustomJSONMarshalOptions{Indent: "  "}.Marshal(&h)
		if err != nil {
			d.t.Fatal(err)
		}
		// Sticky queue names embed the hostname; keep it out of a public repo.
		if host, err := os.Hostname(); err == nil {
			bs = bytes.ReplaceAll(bs, []byte(host), []byte("drill-host"))
		}
		name := filepath.Join("testdata", "history-"+filepath.Base(d.t.Name())+".json")
		if err := os.WriteFile(name, bs, 0o644); err != nil {
			d.t.Fatal(err)
		}
	}
}

// slowFirstLedger applies each key's first write, then stalls past the
// activity timeout, as if the database committed but the reply was lost. The
// retry must find the posting already there.
type slowFirstLedger struct {
	Ledger
	delay time.Duration

	mu    sync.Mutex
	seen  map[string]bool
	calls int
}

func (s *slowFirstLedger) stats() (calls, keys int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls, len(s.seen)
}

func (s *slowFirstLedger) first(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	was := s.seen[key]
	s.seen[key] = true
	return !was
}

func (s *slowFirstLedger) Post(ctx context.Context, key string, entries ...Entry) error {
	_, err := s.PostWithin(ctx, key, nil, 0, entries...)
	return err
}

func (s *slowFirstLedger) PostWithin(ctx context.Context, key string, capped []string, limit int64, entries ...Entry) (bool, error) {
	ok, err := s.Ledger.PostWithin(ctx, key, capped, limit, entries...)
	if err == nil && s.first(key) {
		time.Sleep(s.delay)
	}
	return ok, err
}

// TestReplayGoldenHistories replays recorded drill histories against today's
// workflow code. A change that would break in-flight workflows fails here.
func TestReplayGoldenHistories(t *testing.T) {
	files, _ := filepath.Glob(filepath.Join("testdata", "history-*.json"))
	if len(files) == 0 {
		t.Skip("no golden histories; run the drills with CAS_UPDATE_GOLDEN=1")
	}
	for _, f := range files {
		t.Run(filepath.Base(f), func(t *testing.T) {
			r := worker.NewWorkflowReplayer()
			r.RegisterWorkflow(AuthorizeWorkflow)
			r.RegisterWorkflow(StatementWorkflow)
			r.RegisterWorkflow(AuditWorkflow)
			r.RegisterWorkflow(SubscriptionWorkflow)
			if err := r.ReplayWorkflowHistoryFromJSONFile(nil, f); err != nil {
				t.Error(err)
			}
		})
	}
}
