// Command card-auth-saga runs the Temporal worker for the card authorization
// saga against a local server (`temporal server start-dev`). Set CAS_MYSQL_DSN
// to keep the ledger and card config in MySQL; otherwise both live in memory.
//
//	card-auth-saga                                run the worker
//	card-auth-saga set-limit <card> <cents>       set a card's credit limit (MySQL)
//	card-auth-saga set-rate <merchant> <points>   set a partner's points per $1 (MySQL)
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"strconv"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
)

func main() {
	if err := run(context.Background(), os.Args[1:], os.Getenv("CAS_MYSQL_DSN")); err != nil {
		log.Fatalln(err)
	}
}

func run(ctx context.Context, args []string, dsn string) error {
	if len(args) > 0 {
		change, err := admin(args)
		if err != nil {
			return err
		}
		if dsn == "" {
			return errors.New("set CAS_MYSQL_DSN: card config lives in MySQL")
		}
		l, err := OpenMySQL(ctx, dsn)
		if err != nil {
			return fmt.Errorf("open ledger: %w", err)
		}
		return errors.Join(change(ctx, l), l.Close())
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
		acts = &Activities{Ledger: l, Config: l}
	}

	c, err := client.Dial(client.Options{}) // localhost:7233
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

var errUsage = errors.New("usage: card-auth-saga [set-limit <card> <cents> | set-rate <merchant> <points-per-dollar>]")

// admin parses an admin command into the config change it makes. Bad values
// such as a negative limit are left to the tables' CHECK constraints.
func admin(args []string) (func(context.Context, *MySQLLedger) error, error) {
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
		return func(ctx context.Context, l *MySQLLedger) error { return l.SetCreditLimit(ctx, id, n) }, nil
	case "set-rate":
		return func(ctx context.Context, l *MySQLLedger) error { return l.SetRewardRate(ctx, id, n) }, nil
	}
	return nil, errUsage
}
