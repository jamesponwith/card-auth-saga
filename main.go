// Command card-auth-saga runs the Temporal worker for the card authorization
// saga against a local server (`temporal server start-dev`, or TEMPORAL_ADDRESS).
// Set CAS_MYSQL_DSN to keep the ledger and card config in MySQL; otherwise both
// live in memory.
//
//	card-auth-saga                                run the worker
//	card-auth-saga set-limit <card> <cents>       create or update a card, and start its statements (MySQL)
//	card-auth-saga set-apr <card> <basis-points>  set a card's APR, from its next statement (MySQL)
//	card-auth-saga set-rate <merchant> <points>   set a partner's points per $1 (MySQL)
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"strconv"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
)

func main() {
	if err := run(context.Background(), os.Args[1:], os.Getenv); err != nil {
		log.Fatalln(err)
	}
}

func run(ctx context.Context, args []string, env func(string) string) error {
	dsn := env("CAS_MYSQL_DSN")
	temporalOptions := client.Options{HostPort: env("TEMPORAL_ADDRESS")} // empty: localhost:7233
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
			return cmd(ctx, l, temporalOptions)
		}
		acts = &Activities{Ledger: l, Config: l}
	}

	c, err := client.Dial(temporalOptions)
	if err != nil {
		return fmt.Errorf("dial temporal: %w", err)
	}
	defer c.Close()
	w := worker.New(c, TaskQueue, worker.Options{})
	w.RegisterWorkflow(AuthorizeWorkflow)
	w.RegisterWorkflow(StatementWorkflow)
	w.RegisterActivity(acts)
	return w.Run(worker.InterruptCh())
}

var errUsage = errors.New("usage: card-auth-saga [set-limit <card> <cents> | set-apr <card> <basis-points> | set-rate <merchant> <points-per-dollar>]")

// adminCmd applies a parsed admin command. Temporal options reach set-limit,
// which also starts the card's statements.
type adminCmd func(context.Context, *MySQLLedger, client.Options) error

// admin parses an admin command. Bad values such as a negative limit are left
// to the tables' CHECK constraints.
func admin(args []string) (adminCmd, error) {
	if len(args) != 3 || args[1] == "" {
		return nil, errUsage
	}
	id := args[1]
	n, err := strconv.ParseInt(args[2], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("%s: %q is not a whole number", args[0], args[2])
	}
	switch args[0] {
	case "set-limit":
		return func(ctx context.Context, l *MySQLLedger, opts client.Options) error {
			if err := l.SetCreditLimit(ctx, id, n); err != nil {
				return err
			}
			return startStatement(ctx, opts, id)
		}, nil
	case "set-apr":
		return func(ctx context.Context, l *MySQLLedger, _ client.Options) error { return l.SetAPR(ctx, id, n) }, nil
	case "set-rate":
		return func(ctx context.Context, l *MySQLLedger, _ client.Options) error { return l.SetRewardRate(ctx, id, n) }, nil
	}
	return nil, errUsage
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
