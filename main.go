// Command card-auth-saga runs the Temporal worker for the card authorization
// saga against a local server (`temporal server start-dev`, or TEMPORAL_ADDRESS).
// Set CAS_MYSQL_DSN to keep the ledger and card config in MySQL; otherwise both
// live in memory. The worker serves /healthz on CAS_HTTP_ADDR (default :8080)
// for the Operate stage (ADR 0006), and /debug/vars there too unless
// CAS_DEBUG_ADDR gives the metrics a port of their own.
//
//	card-auth-saga                                run the worker
//	card-auth-saga set-limit <card> <cents>       create or update a card, and start its statements (MySQL)
//	card-auth-saga set-apr <card> <basis-points>  set a card's APR, from its next statement (MySQL)
//	card-auth-saga set-rate <merchant> <points>   set a partner's points per $1 (MySQL)
//	card-auth-saga freeze|unfreeze|close <card>   decline new auths; closing is final (MySQL)
//	card-auth-saga replace <card> <new-number>    a new number on the same account; the old one declines (MySQL)
//	card-auth-saga audit                          run the ledger audit now; exits non-zero on findings
//	card-auth-saga subscribe <id> <card> <merchant> <cents>   charge the card every 30 days, from now
//	card-auth-saga unsubscribe <id>               stop a subscription's future charges
package main

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	ossignal "os/signal" // "signal" is a test helper type in this package
	"slices"
	"strconv"
	"strings"
	"syscall"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
)

func main() {
	ctx, stop := ossignal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:], os.Getenv); err != nil {
		log.Fatalln(err)
	}
}

func run(ctx context.Context, args []string, env func(string) string) error {
	dsn := env("CAS_MYSQL_DSN")
	temporalOptions := client.Options{HostPort: env("TEMPORAL_ADDRESS")} // empty: localhost:7233
	if len(args) == 1 && args[0] == "audit" {
		return audit(ctx, temporalOptions)
	}
	if len(args) > 0 && (args[0] == "subscribe" || args[0] == "unsubscribe") {
		return subscriptionCmd(ctx, args, temporalOptions)
	}
	var cmd adminCmd
	if len(args) > 0 {
		var err error
		if cmd, err = admin(args); err != nil {
			return err
		}
		if dsn == "" {
			return errors.New("set CAS_MYSQL_DSN: card config lives in MySQL")
		}
	}

	var ping func(context.Context) error // the ledger's database, for /healthz
	acts := &Activities{
		Ledger: NewMemLedger(),
		// The in-memory worker is a demo, so its config is too.
		Config: StaticConfig{Limits: map[string]int64{"card-1": 100_000}, Rates: map[string]int64{"shell": 3, "booking": 5}},
	}
	if dsn != "" {
		l, err := OpenMySQL(ctx, dsn)
		if err != nil {
			return fmt.Errorf("open ledger: %w", err)
		}
		defer func() { _ = l.Close() }()
		if cmd != nil {
			if err := cmd(ctx, l, temporalOptions); err != nil {
				return err
			}
			fmt.Println("ok:", strings.Join(args, " ")) // a silent success reads as nothing happened
			return nil
		}
		acts = &Activities{Ledger: l, Config: l}
		ping = l.Ping
	}

	temporalOptions.MetricsHandler = expvarMetrics{m: temporalMetrics}
	c, err := client.Dial(temporalOptions)
	if err != nil {
		return fmt.Errorf("dial temporal: %w", err)
	}
	defer c.Close()
	// Listen before starting work: a worker that can't serve /healthz looks
	// down to the prober, so a taken port should stop it here, loudly.
	ln, err := net.Listen("tcp", cmp.Or(env("CAS_HTTP_ADDR"), ":8080"))
	if err != nil {
		return fmt.Errorf("ops endpoint: %w", err)
	}
	// CAS_DEBUG_ADDR gives the metrics a port of their own, so a deployment can
	// publish /healthz without publishing its internals (deploy/fly/worker.toml).
	debugAddr := env("CAS_DEBUG_ADDR")
	ops := &http.Server{Handler: opsHandler(time.Now(), workerHealth(c, ping), debugAddr == ""), ReadHeaderTimeout: 5 * time.Second}
	go func() { _ = ops.Serve(ln) }()  // returns ErrServerClosed on Close
	defer func() { _ = ops.Close() }() // nothing on a health and metrics port needs draining
	if debugAddr != "" {
		dln, err := net.Listen("tcp", debugAddr)
		if err != nil {
			return fmt.Errorf("debug endpoint: %w", err)
		}
		debug := &http.Server{Handler: debugHandler(), ReadHeaderTimeout: 5 * time.Second}
		go func() { _ = debug.Serve(dln) }()
		defer func() { _ = debug.Close() }()
	}
	// The SDK's 2 workflow-task pollers each wait a network round trip per task.
	// Over Fly's private network that capped the worker near 14 tasks/s, about 2
	// auths/s, with decisions queued for seconds (docs/writeup.md, "On Fly").
	// ponytail: fixed counts; NewPollerBehaviorAutoscaling if one worker size stops fitting all loads.
	w := worker.New(c, TaskQueue, worker.Options{MaxConcurrentWorkflowTaskPollers: 16, MaxConcurrentActivityTaskPollers: 8})
	w.RegisterWorkflow(AuthorizeWorkflow)
	w.RegisterWorkflow(StatementWorkflow)
	w.RegisterWorkflow(AuditWorkflow)
	w.RegisterWorkflow(SubscriptionWorkflow)
	acts.Temporal = c
	w.RegisterActivity(acts)
	if err := ensureCardIDAttribute(ctx, c, temporalOptions.Namespace); err != nil {
		return fmt.Errorf("search attribute: %w", err)
	}
	if err := ensureAuditSchedule(ctx, c); err != nil {
		return fmt.Errorf("audit schedule: %w", err)
	}
	stopped := make(chan any)
	go func() { <-ctx.Done(); close(stopped) }()
	return w.Run(stopped) // until ctx is cancelled: SIGINT or SIGTERM in main
}

var errUsage = errors.New("usage: card-auth-saga [set-limit <card> <cents> | set-apr <card> <basis-points> | set-rate <merchant> <points-per-dollar> | freeze|unfreeze|close <card> | replace <card> <new-number> | subscribe <id> <card> <merchant> <cents> | unsubscribe <id> | audit]")

// adminCmd applies a parsed admin command. Temporal options reach set-limit,
// which also starts the card's statements.
type adminCmd func(context.Context, *MySQLLedger, client.Options) error

// admin parses an admin command. Bad values such as a negative limit are left
// to the tables' CHECK constraints. A card may be named by any of its numbers:
// the command acts on its account (SPEC M18).
func admin(args []string) (adminCmd, error) {
	statuses := map[string]string{"freeze": CardFrozen, "unfreeze": CardActive, "close": CardClosed}
	if status, ok := statuses[args[0]]; ok && len(args) == 2 && args[1] != "" {
		id := args[1]
		return onAccount(id, func(ctx context.Context, l *MySQLLedger, _ client.Options, account string) error {
			return l.SetCardStatus(ctx, account, status)
		}), nil
	}
	if len(args) != 3 || args[1] == "" || args[2] == "" || !slices.Contains([]string{"set-limit", "set-apr", "set-rate", "replace"}, args[0]) {
		return nil, errUsage
	}
	id := args[1]
	cardIDs := args[1:2]
	switch args[0] {
	case "set-rate": // merchants aren't card IDs
		cardIDs = nil
	case "replace":
		cardIDs = args[1:]
	}
	for _, c := range cardIDs {
		if err := checkCardID(args[0], c); err != nil {
			return nil, err
		}
	}
	if args[0] == "replace" {
		return func(ctx context.Context, l *MySQLLedger, _ client.Options) error {
			return l.ReplaceCard(ctx, id, args[2])
		}, nil
	}
	n, err := strconv.ParseInt(args[2], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("%s: %q is not a whole number", args[0], args[2])
	}
	switch args[0] {
	case "set-limit":
		return onAccount(id, func(ctx context.Context, l *MySQLLedger, opts client.Options, account string) error {
			if err := l.SetCreditLimit(ctx, account, n); err != nil {
				return err
			}
			return startStatement(ctx, opts, account)
		}), nil
	case "set-apr":
		return onAccount(id, func(ctx context.Context, l *MySQLLedger, _ client.Options, account string) error {
			return l.SetAPR(ctx, account, n)
		}), nil
	case "set-rate":
		return func(ctx context.Context, l *MySQLLedger, _ client.Options) error { return l.SetRewardRate(ctx, id, n) }, nil
	}
	return nil, errUsage
}

// checkCardID refuses what a card ID can't hold: account IDs are
// card:<id>:<kind>, and the audit quotes card IDs in visibility queries.
func checkCardID(cmd, id string) error {
	if strings.ContainsAny(id, `:'"\`) {
		return fmt.Errorf("%s: card ID %q can't contain : ' \" or \\", cmd, id)
	}
	return nil
}

// onAccount runs f on the account a card number belongs to. A new ID is its
// own account, so set-limit can create it.
func onAccount(number string, f func(context.Context, *MySQLLedger, client.Options, string) error) adminCmd {
	return func(ctx context.Context, l *MySQLLedger, opts client.Options) error {
		account, _, err := l.CardAccount(ctx, number)
		if err != nil {
			return err
		}
		return f(ctx, l, opts, account)
	}
}

// startStatement starts the card's statement cycle, or leaves the running one
// alone: one StatementWorkflow per card, for the card's life. It is idempotent,
// so a set-limit that saved the card but failed here is fixed by running it again.
func startStatement(ctx context.Context, opts client.Options, card string) error {
	c, err := client.Dial(opts)
	if err == nil {
		defer c.Close()
		_, err = c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
			ID:                       StatementWorkflowID(card),
			TaskQueue:                TaskQueue,
			WorkflowIDConflictPolicy: enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING,
		}, StatementWorkflow, StatementTerms{CardID: card}, Statement{})
	}
	if err != nil {
		return fmt.Errorf("card saved; its statements did not start (run set-limit again): %w", err)
	}
	return nil
}

// audit runs the ledger audit now and prints what it finds. It needs a running
// worker; the error on findings makes the command exit non-zero.
func audit(ctx context.Context, opts client.Options) error {
	c, err := client.Dial(opts)
	if err != nil {
		return fmt.Errorf("dial temporal: %w", err)
	}
	defer c.Close()
	run, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID: AuditScheduleID + "-manual", TaskQueue: TaskQueue,
		WorkflowIDConflictPolicy: enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING, // one at a time
	}, AuditWorkflow, AuditOptions{})
	if err != nil {
		return err
	}
	var report AuditReport
	if err := run.Get(ctx, &report); err != nil {
		return fmt.Errorf("audit: %w", err)
	}
	if len(report.Findings) == 0 {
		fmt.Println("audit: clean")
		return nil
	}
	lines := make([]string, len(report.Findings))
	for i, f := range report.Findings {
		lines[i] = fmt.Sprintf("  %s %s: %s", f.Check, f.Subject, f.Detail)
	}
	return fmt.Errorf("audit: %d findings\n%s", len(report.Findings), strings.Join(lines, "\n"))
}

// subscriptionCmd starts a subscription, which charges at once and then every
// cycle, or cancels one. A subscription ID is used once, ever.
func subscriptionCmd(ctx context.Context, args []string, opts client.Options) error {
	subscribe := args[0] == "subscribe" && len(args) == 5
	if !subscribe && !(args[0] == "unsubscribe" && len(args) == 2) || slices.Contains(args[1:], "") {
		return errUsage
	}
	var terms SubscriptionTerms
	if subscribe {
		terms = SubscriptionTerms{ID: args[1], CardID: args[2], Merchant: args[3]}
		var err error
		if terms.Amount, err = strconv.ParseInt(args[4], 10, 64); err != nil || terms.Amount <= 0 {
			return fmt.Errorf("subscribe: %q is not a positive number of cents", args[4])
		}
		if err := checkCardID(args[0], terms.CardID); err != nil {
			return err
		}
	}
	c, err := client.Dial(opts)
	if err != nil {
		return fmt.Errorf("dial temporal: %w", err)
	}
	defer c.Close()
	id := SubscriptionWorkflowID(args[1])
	if subscribe {
		// Without ErrorWhenAlreadyStarted the client hands back the existing run, and a reused ID looks like success.
		_, err = c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: id, TaskQueue: TaskQueue,
			WorkflowIDReusePolicy: enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE, WorkflowExecutionErrorWhenAlreadyStarted: true},
			SubscriptionWorkflow, terms, Subscription{})
	} else {
		var h client.WorkflowUpdateHandle
		h, err = c.UpdateWorkflow(ctx, client.UpdateWorkflowOptions{WorkflowID: id, UpdateName: CancelUpdate, WaitForStage: client.WorkflowUpdateStageCompleted})
		if err == nil {
			err = h.Get(ctx, nil)
		}
	}
	if err != nil {
		return fmt.Errorf("%s %s: %w", args[0], args[1], err)
	}
	fmt.Println("ok:", strings.Join(args, " "))
	return nil
}

// ensureCardIDAttribute registers the CardID search attribute, which every
// new auth sets on itself: without it, their first workflow task would fail.
func ensureCardIDAttribute(ctx context.Context, c client.Client, namespace string) error {
	_, err := c.OperatorService().AddSearchAttributes(ctx, &operatorservice.AddSearchAttributesRequest{
		Namespace:        cmp.Or(namespace, client.DefaultNamespace),
		SearchAttributes: map[string]enumspb.IndexedValueType{CardIDAttribute.GetName(): enumspb.INDEXED_VALUE_TYPE_KEYWORD},
	})
	var exists *serviceerror.AlreadyExists
	if errors.As(err, &exists) {
		return nil
	}
	return err
}
