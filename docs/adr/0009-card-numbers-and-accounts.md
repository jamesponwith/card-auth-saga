# 0009. A card's first number is its account; later numbers map to it

Date: 2026-10-18
Status: accepted

## Context

A lost card needs a new number on the same account: same limit, balance, statement cycle and points.
Everything money-related is keyed by the card ID: ledger accounts (`card:<id>:posted`), `statement:<id>`,
the `cards` row, velocity attempts, the `CardID` search attribute and the audit. Renaming accounts to
a new account ID would mean migrating balances, workflow IDs and history, all of it live.

## Decision

The ID a card is created with stays its account ID forever. `card_numbers(number, account,
replaced_at)` maps every number, the first one included, to its account. An auth resolves the number
it was given once, up front (`GetVersion("card-replacement")`), declines a replaced number, and
otherwise carries the account in `AuthRequest.CardID`, so every later step uses the account unchanged.

## Consequences

Nothing already in the ledger or in Temporal changes. The account ID is a card number the cardholder
may no longer hold, so it should be treated as an internal ID. Velocity counts per account, not per
number. The number table's primary key keeps one number from ever belonging to two accounts.
