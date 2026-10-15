package main

// Failure drills: the real worker against a real Temporal dev server, which the
// SDK downloads and starts. Skipped under -short. Each drill checks the ledger
// is exactly right afterwards, then replays the recorded history to prove the
// workflow is deterministic. CAS_UPDATE_GOLDEN=1 saves those histories to
// testdata/, where TestReplayGoldenHistories replays them on every commit.

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/temporalproto"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
)

func TestDrills(t *testing.T) {
	if testing.Short() {
		t.Skip("drills start a Temporal dev server; run without -short")
	}
	ctx := context.Background()
	srv, err := testsuite.StartDevServer(ctx, testsuite.DevServerOptions{LogLevel: "error", ClientOptions: &client.Options{Identity: "drill-client"}})
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
			if !d.authorize(req) {
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
		if !d.authorize(AuthRequest{Amount: 40_000, RefundWindow: time.Second}) {
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
		if !d.authorize(AuthRequest{Amount: 40_000, RefundWindow: time.Second}) {
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

	t.Run("update with start declined", func(t *testing.T) {
		d := newDrill(t, c, "uws-decline", nil)
		d.startWorker()
		if d.authorize(AuthRequest{Amount: 100_001}) {
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

// authorize starts the auth (or attaches to the running one) and waits for the decision.
func (d *drill) authorize(req AuthRequest) bool {
	d.t.Helper()
	req.CardID, req.Merchant = d.card, d.merchant
	ctx := context.Background()
	start := d.c.NewWithStartWorkflowOperation(client.StartWorkflowOptions{
		ID: d.card, TaskQueue: TaskQueue, WorkflowIDConflictPolicy: enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING,
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
	return dec.Approved
}

// update sends an update to the drill's workflow and waits for it to finish.
func (d *drill) update(name, id string, arg any) error { return d.updateResult(name, id, arg, nil) }

// updateResult is update, decoding the update's result into result.
func (d *drill) updateResult(name, id string, arg, result any) error {
	h, err := d.c.UpdateWorkflow(context.Background(), client.UpdateWorkflowOptions{
		WorkflowID: d.card, UpdateID: id, UpdateName: name, Args: []any{arg},
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
			if err := r.ReplayWorkflowHistoryFromJSONFile(nil, f); err != nil {
				t.Error(err)
			}
		})
	}
}
