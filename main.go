// Command card-auth-saga runs the Temporal worker for the card authorization
// saga against a local server (`temporal server start-dev`). Set CAS_MYSQL_DSN
// to keep the ledger in MySQL; otherwise it lives in memory.
package main

import (
	"context"
	"log"
	"os"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
)

func main() {
	var ledger Ledger = NewMemLedger()
	if dsn := os.Getenv("CAS_MYSQL_DSN"); dsn != "" {
		l, err := OpenMySQL(context.Background(), dsn)
		if err != nil {
			log.Fatalln("open ledger:", err)
		}
		defer func() { _ = l.Close() }()
		ledger = l
	}

	c, err := client.Dial(client.Options{}) // localhost:7233
	if err != nil {
		log.Fatalln("dial temporal:", err)
	}
	defer c.Close()

	w := worker.New(c, TaskQueue, worker.Options{})
	w.RegisterWorkflow(AuthorizeWorkflow)
	// ponytail: hard-coded demo limits and partner rates until they get their own tables.
	w.RegisterActivity(&Activities{
		Ledger: ledger,
		Limits: map[string]int64{"card-1": 100_000},
		Rates:  map[string]int64{"shell": 3, "booking": 5},
	})
	if err := w.Run(worker.InterruptCh()); err != nil {
		log.Fatalln(err)
	}
}
