package main

// What a card network does: send an auth and wait for the decision, give up
// at a deadline and reverse, and capture. Subscriptions charge through these,
// and the authorize command plays the network by hand.

import (
	"context"
	"errors"
	"fmt"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
)

// DecisionDeadline is how long a network waits for a decision before it
// declines the auth itself and sends a timeout reversal.
const DecisionDeadline = 2 * time.Second

// startAuth starts auth id, or attaches to it while it runs. An auth ID is used
// once: one that finished (a decline, say) is refused rather than tried again.
func startAuth(c client.Client, id string, req AuthRequest) client.WithStartWorkflowOperation {
	return c.NewWithStartWorkflowOperation(client.StartWorkflowOptions{
		ID: id, TaskQueue: TaskQueue,
		WorkflowIDConflictPolicy: enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING,
		WorkflowIDReusePolicy:    enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
	}, AuthorizeWorkflow, req)
}

// decide sends auth id's authorize, starting the auth if need be, and waits for the decision.
func decide(ctx context.Context, c client.Client, id string, req AuthRequest) (Decision, error) {
	var dec Decision
	h, err := c.UpdateWithStartWorkflow(ctx, client.UpdateWithStartWorkflowOptions{
		StartWorkflowOperation: startAuth(c, id, req),
		UpdateOptions:          client.UpdateWorkflowOptions{UpdateName: AuthorizeUpdate, WaitForStage: client.WorkflowUpdateStageCompleted},
	})
	if err == nil {
		err = h.Get(ctx, &dec)
	}
	return dec, err
}

// authorizeWithin asks for a decision and waits at most deadline. Without an
// answer, for any reason, it sends a timeout reversal, as a network does, so
// whatever the auth decides later no hold of it stands (SPEC M20). It reports
// DeclineTimeout, or the auth's own error (a bad amount, say) once reversed.
func authorizeWithin(ctx context.Context, c client.Client, id string, req AuthRequest, deadline time.Duration) (Decision, error) {
	dctx, cancel := context.WithTimeout(ctx, deadline)
	defer cancel()
	dec, err := decide(dctx, c, id, req)
	if err == nil {
		return dec, nil
	}
	// Not the deadline's context: the server can give up on the wait a moment
	// before it, and a caller that gave up still mustn't leave a hold behind.
	rctx, rcancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	defer rcancel()
	if rerr := reverseAuth(rctx, c, id, req); rerr != nil {
		return Decision{}, fmt.Errorf("no decision (%w), and the timeout reversal failed: %w", err, rerr)
	}
	var failed *temporal.ApplicationError
	if errors.As(err, &failed) {
		return Decision{}, err
	}
	return Decision{Reason: DeclineTimeout}, nil
}

// reverseAuth sends a timeout reversal for auth id. It starts the auth if the
// network's request never got there, so a start still in flight finds it
// already reversed. An auth that finished, or holds nothing, has nothing to
// reverse; a fixed update ID makes a resend a no-op.
func reverseAuth(ctx context.Context, c client.Client, id string, req AuthRequest) error {
	h, err := c.UpdateWithStartWorkflow(ctx, client.UpdateWithStartWorkflowOptions{
		StartWorkflowOperation: startAuth(c, id, req),
		UpdateOptions: client.UpdateWorkflowOptions{UpdateID: "timeout-reversal", UpdateName: ReverseSignal,
			WaitForStage: client.WorkflowUpdateStageCompleted},
	})
	if err == nil {
		err = h.Get(ctx, nil)
	}
	var finished *serviceerror.WorkflowExecutionAlreadyStarted
	var rejected *temporal.ApplicationError // the validator: declined, expired or already reversed
	if errors.As(err, &finished) || errors.As(err, &rejected) {
		return nil
	}
	return err
}

// authorizeAndCapture asks for a decision and, if approved, captures in full.
// decided is how long the decision took, the latency a network waits on.
func authorizeAndCapture(ctx context.Context, c client.Client, id string, req AuthRequest) (dec Decision, decided time.Duration, err error) {
	began := time.Now()
	dec, err = decide(ctx, c, id, req)
	decided = time.Since(began)
	if err != nil || !dec.Approved {
		return dec, decided, err
	}
	ch, err := c.UpdateWorkflow(ctx, client.UpdateWorkflowOptions{
		WorkflowID: id, UpdateID: "capture", UpdateName: CaptureSignal, Args: []any{Capture{Amount: req.Amount}},
		WaitForStage: client.WorkflowUpdateStageCompleted,
	})
	if err != nil {
		return dec, decided, err
	}
	return dec, decided, ch.Get(ctx, nil)
}
