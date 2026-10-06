// Command card-auth-saga runs the Temporal worker for the card authorization
// saga against a local server (`temporal server start-dev`).
package main

import (
	"log"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
)

func main() {
	c, err := client.Dial(client.Options{}) // localhost:7233
	if err != nil {
		log.Fatalln("dial temporal:", err)
	}
	defer c.Close()

	w := worker.New(c, TaskQueue, worker.Options{})
	w.RegisterWorkflow(AuthorizeWorkflow)
	// ponytail: hard-coded demo limits and a process-local ledger until M3 moves both to MySQL.
	w.RegisterActivity(&Activities{Ledger: NewLedger(), Limits: map[string]int64{"card-1": 100_000}})
	if err := w.Run(worker.InterruptCh()); err != nil {
		log.Fatalln(err)
	}
}
