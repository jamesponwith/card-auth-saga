# 0010. Subscriptions charge through the Temporal client, from an activity

Date: 2026-10-18
Status: accepted

## Context

A subscription charges a card every cycle, and each charge must be a real auth: the same limit,
velocity, status and replacement checks, ledger postings, rewards, refund window and audit as one a
network sends. A workflow can't send an update to another workflow (only signals), and the decision
comes back from an update. A child `AuthorizeWorkflow` would tie each auth's 30-day life to its
subscription's run, which continues as new every cycle.

## Decision

`ChargeCard` is an activity that does what a network does: Update-with-Start `authorize`, then a
`capture` update, through the worker's Temporal client. Auth IDs are `<subscription>:c<cycle>:a<attempt>`
and the capture has a fixed update ID, so a retried activity attaches to the same auth and capture.

## Consequences

Subscription charges are ordinary auths: everything that inspects auths sees them, with nothing
special-cased. The activity needs a Temporal client (`Activities.Temporal`), as the audit's does. A
charge is two hops instead of one, which a monthly charge can afford.
