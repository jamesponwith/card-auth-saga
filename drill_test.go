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
	var l Ledger = NewMemLedger()
	if dsn := os.Getenv("CAS_MYSQL_DSN"); dsn != "" {
		ml, err := OpenMySQL(context.Background(), dsn)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = ml.Close() })
		l = ml
	}
	if wrap != nil {
		wrap.Ledger = l
		l = wrap
	}
	card := fmt.Sprintf("drill-%s-%d", name, runID)
	return &drill{
		t: t, c: c, l: l, card: card, merchant: card + "-shop",
		acts: &Activities{Ledger: l, Limits: map[string]int64{card: 100_000}},
	}
}

func (d *drill) startWorker() worker.Worker {
	w := worker.New(d.c, TaskQueue, worker.Options{Identity: "drill-worker"})
	w.RegisterWorkflow(AuthorizeWorkflow)
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
			if err := r.ReplayWorkflowHistoryFromJSONFile(nil, f); err != nil {
				t.Error(err)
			}
		})
	}
}
