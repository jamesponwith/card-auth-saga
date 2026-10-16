package main

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

const (
	AuditScheduleID = "ledger-audit"
	AuditInterval   = 24 * time.Hour

	// The longest an auth can legitimately run: its hold, the refund window,
	// and both dispute deadlines (7 + 30 + 45 + 30 days), rounded up.
	DefaultStuckAuth = 120 * 24 * time.Hour
	// A statement run lasts one cycle; two means it's stuck.
	DefaultStuckStatement = 2 * DefaultCycle
)

// Audit checks, as findings name them.
const (
	CheckCardMoney      = "card-money"
	CheckHolds          = "holds"
	CheckPoints         = "points"
	CheckPointsNet      = "points-net"
	CheckPaymentMemos   = "payment-memos"
	CheckLedgerTotal    = "ledger-total"
	CheckStuckAuth      = "stuck-auth"
	CheckStuckStatement = "stuck-statement"
	CheckFailedWorkflow = "failed-workflow"
	CheckOrphanedHold   = "orphaned-hold"
)

// Finding is one broken invariant. Subject is a card ID or a workflow, empty
// for a ledger-wide check.
type Finding struct {
	Check, Subject, Detail string
}

// AuditReport is AuditWorkflow's result.
type AuditReport struct {
	At       time.Time
	Findings []Finding
}

// AuditOptions tune the workflow checks; zero takes the default. Drills
// shrink StuckAuth to see a live hold reported.
type AuditOptions struct {
	StuckAuth time.Duration
}

// AuditWorkflow checks the ledger's invariants and the workflows' health, and
// reports what it finds. It changes nothing: a finding is for a human.
func AuditWorkflow(ctx workflow.Context, opts AuditOptions) (AuditReport, error) {
	// Reading the whole ledger and every running auth takes a while.
	ctx = workflow.WithStartToCloseTimeout(withLedger(ctx), 10*time.Minute)
	var a *Activities
	now := workflow.Now(ctx)
	report := AuditReport{At: now}

	var ledger []Finding
	if err := workflow.ExecuteActivity(ctx, a.AuditLedger).Get(ctx, &ledger); err != nil {
		return report, err
	}
	var workflows []Finding
	cut := AuditCutoffs{
		StuckAuthBefore:      now.Add(-cmp.Or(opts.StuckAuth, DefaultStuckAuth)),
		StuckStatementBefore: now.Add(-DefaultStuckStatement),
		FailedAfter:          now.Add(-AuditInterval), // since the previous audit
	}
	if err := workflow.ExecuteActivity(ctx, a.AuditWorkflows, cut).Get(ctx, &workflows); err != nil {
		return report, err
	}
	report.Findings = append(ledger, workflows...)
	for _, f := range report.Findings {
		workflow.GetLogger(ctx).Error("audit finding", "check", f.Check, "subject", f.Subject, "detail", f.Detail)
	}
	return report, nil
}

// AuditLedger reads every account and checks the ledger's invariants.
func (a *Activities) AuditLedger(ctx context.Context) ([]Finding, error) {
	accounts, err := a.Ledger.Accounts(ctx)
	if err != nil {
		return nil, err
	}
	return auditLedger(accounts), nil
}

// auditLedger checks the invariants that every posting keeps (SPEC M15):
// per card, money nets to zero and holds and points never go negative; payment
// memos net to zero; points net to zero against the partners; and the whole
// ledger sums to zero.
func auditLedger(accounts map[string]int64) []Finding {
	type card struct{ money, holds, points, memos int64 }
	cards := map[string]*card{}
	var total, pointsNet int64
	for id, b := range accounts {
		total += b
		if strings.HasPrefix(id, "partner:") && strings.HasSuffix(id, ":points-issued") {
			pointsNet += b
			continue
		}
		rest, ok := strings.CutPrefix(id, "card:")
		if !ok {
			continue
		}
		cardID, kind, ok := strings.Cut(rest, ":")
		if !ok {
			continue
		}
		c := cards[cardID]
		if c == nil {
			c = &card{}
			cards[cardID] = c
		}
		switch {
		case kind == "holds":
			c.money += b
			c.holds += b
		case kind == "posted" || kind == "open-to-buy":
			c.money += b
		case kind == "points":
			c.points += b
			pointsNet += b
		case kind == "payments" || strings.HasPrefix(kind, "paid:"):
			c.memos += b
		}
	}

	var findings []Finding
	for _, id := range slices.Sorted(maps.Keys(cards)) {
		c := cards[id]
		if c.money != 0 {
			findings = append(findings, Finding{CheckCardMoney, id, fmt.Sprintf("holds + posted + open-to-buy = %d", c.money)})
		}
		if c.holds < 0 {
			findings = append(findings, Finding{CheckHolds, id, fmt.Sprintf("holds = %d", c.holds)})
		}
		if c.points < 0 {
			findings = append(findings, Finding{CheckPoints, id, fmt.Sprintf("points = %d", c.points)})
		}
		if c.memos != 0 {
			findings = append(findings, Finding{CheckPaymentMemos, id, fmt.Sprintf("paid + payments = %d", c.memos)})
		}
	}
	if pointsNet != 0 {
		findings = append(findings, Finding{CheckPointsNet, "", fmt.Sprintf("card points + partner points-issued = %d", pointsNet)})
	}
	if total != 0 {
		findings = append(findings, Finding{CheckLedgerTotal, "", fmt.Sprintf("all accounts = %d", total)})
	}
	return findings
}

// AuditCutoffs are the times the workflow checks compare against, fixed by
// the workflow so a retried activity asks the same questions.
type AuditCutoffs struct {
	StuckAuthBefore, StuckStatementBefore, FailedAfter time.Time
}

// AuditWorkflows finds auths and statement runs past their longest life, and
// workflows that failed since the last audit, through Temporal's visibility
// queries. Without a Temporal client (unit tests) it checks nothing.
func (a *Activities) AuditWorkflows(ctx context.Context, cut AuditCutoffs) ([]Finding, error) {
	if a.Temporal == nil {
		return nil, nil
	}
	findings, err := a.orphanedHolds(ctx)
	if err != nil {
		return nil, err
	}
	ts := func(t time.Time) string { return "'" + t.UTC().Format(time.RFC3339Nano) + "'" }
	checks := []struct{ check, query string }{
		{CheckStuckAuth, "WorkflowType = 'AuthorizeWorkflow' AND ExecutionStatus = 'Running' AND StartTime < " + ts(cut.StuckAuthBefore)},
		{CheckStuckStatement, "WorkflowType = 'StatementWorkflow' AND ExecutionStatus = 'Running' AND StartTime < " + ts(cut.StuckStatementBefore)},
		{CheckFailedWorkflow, "(ExecutionStatus = 'Failed' OR ExecutionStatus = 'TimedOut') AND CloseTime > " + ts(cut.FailedAfter)},
	}
	for _, ch := range checks {
		var token []byte
		for {
			res, err := a.Temporal.ListWorkflow(ctx, &workflowservice.ListWorkflowExecutionsRequest{Query: ch.query, NextPageToken: token})
			if err != nil {
				return nil, fmt.Errorf("%s: %w", ch.check, err)
			}
			for _, w := range res.GetExecutions() {
				detail := fmt.Sprintf("%s started %s", w.GetType().GetName(), w.GetStartTime().AsTime().UTC().Format(time.RFC3339))
				if ch.check == CheckFailedWorkflow {
					detail = fmt.Sprintf("%s %s at %s", w.GetType().GetName(), statusName(w.GetStatus()), w.GetCloseTime().AsTime().UTC().Format(time.RFC3339))
				}
				findings = append(findings, Finding{ch.check, w.GetExecution().GetWorkflowId(), detail})
			}
			if token = res.GetNextPageToken(); len(token) == 0 {
				break
			}
		}
	}
	return findings, nil
}

func statusName(s enumspb.WorkflowExecutionStatus) string {
	return strings.ToLower(strings.TrimPrefix(s.String(), "WORKFLOW_EXECUTION_STATUS_"))
}

// ensureAuditSchedule creates the nightly audit schedule if it's missing and
// leaves an existing one (and any edits to it) alone.
func ensureAuditSchedule(ctx context.Context, c client.Client) error {
	_, err := c.ScheduleClient().Create(ctx, client.ScheduleOptions{
		ID:      AuditScheduleID,
		Spec:    client.ScheduleSpec{Intervals: []client.ScheduleIntervalSpec{{Every: AuditInterval}}},
		Overlap: enumspb.SCHEDULE_OVERLAP_POLICY_SKIP,
		Action: &client.ScheduleWorkflowAction{
			ID: AuditScheduleID, Workflow: AuditWorkflow, Args: []any{AuditOptions{}}, TaskQueue: TaskQueue,
		},
	})
	if errors.Is(err, temporal.ErrScheduleAlreadyRunning) {
		return nil
	}
	return err
}

// lastAuditFindings is the findings of the most recent finished scheduled
// audit: none before the first one finishes, or with no schedule at all.
func lastAuditFindings(ctx context.Context, c client.Client) ([]Finding, error) {
	desc, err := c.ScheduleClient().GetHandle(ctx, AuditScheduleID).Describe(ctx)
	var notFound *serviceerror.NotFound
	if errors.As(err, &notFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	actions := desc.Info.RecentActions
	for i := len(actions) - 1; i >= 0; i-- {
		run := actions[i].StartWorkflowResult
		if run == nil {
			continue
		}
		info, err := c.DescribeWorkflowExecution(ctx, run.WorkflowID, run.FirstExecutionRunID)
		if err != nil {
			return nil, err
		}
		if info.GetWorkflowExecutionInfo().GetStatus() == enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING {
			continue // still auditing: the previous one stands
		}
		var report AuditReport
		if err := c.GetWorkflow(ctx, run.WorkflowID, run.FirstExecutionRunID).Get(ctx, &report); err != nil {
			return nil, fmt.Errorf("the last audit failed: %w", err)
		}
		return report.Findings, nil
	}
	return nil, nil
}

// untaggedAuths counts running auths from before CardID tagging, which a
// card-scoped query can't see.
func (a *Activities) untaggedAuths(ctx context.Context) (int64, error) {
	return a.countWorkflows(ctx, fmt.Sprintf("WorkflowType = 'AuthorizeWorkflow' AND ExecutionStatus = 'Running' AND %s IS NULL", CardIDAttribute.GetName()))
}

func (a *Activities) countWorkflows(ctx context.Context, query string) (int64, error) {
	res, err := a.Temporal.CountWorkflow(ctx, &workflowservice.CountWorkflowExecutionsRequest{Query: query})
	return res.GetCount(), err
}

// orphanRecheck is how long suspected orphaned holds wait before they are
// checked again: long enough for a just-started auth to reach visibility.
var orphanRecheck = 2 * time.Second

// orphanedHolds finds cards holding money with no auth running on them: a
// hold nothing will ever capture or release. It lists the running auths once
// (not one query per card), and reports only cards that look orphaned on two
// passes orphanRecheck apart, so an auth that settled or started mid-audit
// isn't one.
func (a *Activities) orphanedHolds(ctx context.Context) ([]Finding, error) {
	if n, err := a.untaggedAuths(ctx); err != nil || n > 0 {
		return nil, err // auths from before tagging can't be attributed yet
	}
	first, err := a.unclaimedHolds(ctx)
	if err != nil || len(first) == 0 {
		return nil, err
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(orphanRecheck):
	}
	second, err := a.unclaimedHolds(ctx)
	if err != nil {
		return nil, err
	}
	var findings []Finding
	for _, card := range slices.Sorted(maps.Keys(second)) {
		if first[card] > 0 {
			findings = append(findings, Finding{CheckOrphanedHold, card, fmt.Sprintf("holds %d with no auth running", second[card])})
		}
	}
	return findings, nil
}

// unclaimedHolds is each card holding money with no running auth, and how much.
// ponytail: reads every account and lists every running auth in memory;
// page both by card when the ledger outgrows a worker.
func (a *Activities) unclaimedHolds(ctx context.Context) (map[string]int64, error) {
	running := map[string]bool{}
	query := "WorkflowType = 'AuthorizeWorkflow' AND ExecutionStatus = 'Running'"
	for token := []byte(nil); ; {
		res, err := a.Temporal.ListWorkflow(ctx, &workflowservice.ListWorkflowExecutionsRequest{Query: query, NextPageToken: token})
		if err != nil {
			return nil, err
		}
		for _, w := range res.GetExecutions() {
			if p := w.GetSearchAttributes().GetIndexedFields()[CardIDAttribute.GetName()]; p != nil {
				var card string
				if err := converter.GetDefaultDataConverter().FromPayload(p, &card); err != nil {
					return nil, err
				}
				running[card] = true
			}
		}
		if token = res.GetNextPageToken(); len(token) == 0 {
			break
		}
	}
	accounts, err := a.Ledger.Accounts(ctx)
	if err != nil {
		return nil, err
	}
	unclaimed := map[string]int64{}
	for id, b := range accounts {
		rest, ok := strings.CutPrefix(id, "card:")
		card, isHolds := strings.CutSuffix(rest, ":holds")
		if ok && isHolds && b > 0 && !strings.Contains(card, ":") && !running[card] {
			unclaimed[card] = b
		}
	}
	return unclaimed, nil
}
