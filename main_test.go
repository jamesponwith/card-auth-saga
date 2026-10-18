package main

import (
	"context"
	"strings"
	"testing"
)

// Admin commands are checked before anything touches the database.
func TestAdminArgs(t *testing.T) {
	tests := []struct {
		args    []string
		wantErr string // substring; empty for a valid command
	}{
		{[]string{"set-limit", "card-1", "100000"}, ""},
		{[]string{"set-rate", "shell", "3"}, ""},
		{[]string{"set-apr", "card-1", "1999"}, ""},
		{[]string{"set-limit", "card-1", "-5"}, ""}, // parsed; the CHECK constraint rejects it
		{[]string{"set-limit", "card-1"}, "usage"},
		{[]string{"set-limit", "", "100"}, "usage"},
		{[]string{"set-limit", "card-1", "1e5"}, "not a whole number"},
		{[]string{"set-limit", "card-1", "100.50"}, "not a whole number"},
		{[]string{"set-limit", "card-1", "99999999999999999999"}, "not a whole number"},
		{[]string{"delete-card", "card-1", "1"}, "usage"},
		{[]string{"set-limit", "card:1", "100"}, "can't contain"},
		{[]string{"set-limit", "o'brien", "100"}, "can't contain"},
		{[]string{"set-rate", "shop:eu", "3"}, ""}, // merchants aren't card IDs
		{[]string{"freeze", "card-1"}, ""},
		{[]string{"unfreeze", "card-1"}, ""},
		{[]string{"close", "card-1"}, ""},
		{[]string{"close"}, "usage"},
		{[]string{"close", "card-1", "now"}, "usage"},
		{[]string{"replace", "card-1", "card-2"}, ""},
		{[]string{"replace", "card-1"}, "usage"},
		{[]string{"replace", "card-1", ""}, "usage"},
		{[]string{"replace", "card-1", "card:2"}, "can't contain"},
		{[]string{"replace", "card-1", "o'brien"}, "can't contain"},
	}
	for _, tt := range tests {
		t.Run(strings.Join(tt.args, " "), func(t *testing.T) {
			_, err := admin(tt.args)
			if (err == nil) != (tt.wantErr == "") || err != nil && !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("admin(%q) = %v, want error containing %q", tt.args, err, tt.wantErr)
			}
		})
	}
}

// envOf is an environment for run, from a map.
func envOf(vars map[string]string) func(string) string {
	return func(k string) string { return vars[k] }
}

// The admin commands write the config the activities read. set-limit also
// starts statements; TestDrills covers that against a real Temporal server.
func TestAdminCommands(t *testing.T) {
	dsn := freshDB(t)
	ctx := context.Background()
	env := envOf(map[string]string{"CAS_MYSQL_DSN": dsn})
	l := open(t, dsn)
	if err := l.SetCreditLimit(ctx, "card-1", 100_000); err != nil { // the card, without set-limit's statement start
		t.Fatal(err)
	}
	for _, args := range [][]string{{"set-apr", "card-1", "1999"}, {"set-rate", "shell", "4"}} {
		if err := run(ctx, args, env); err != nil {
			t.Fatalf("run %q: %v", args, err)
		}
	}
	for _, args := range [][]string{{"set-limit", "card-1", "-1"}, {"set-apr", "card-1", "-1"}, {"set-apr", "no-such-card", "1999"}} {
		if err := run(ctx, args, env); err == nil {
			t.Errorf("run %q succeeded, want an error", args)
		}
	}
	if err := run(ctx, []string{"set-limit", "card-1", "1"}, envOf(nil)); err == nil || !strings.Contains(err.Error(), "CAS_MYSQL_DSN") {
		t.Errorf("without a DSN: %v, want a pointer to CAS_MYSQL_DSN", err)
	}
	// Temporal unreachable: the limit is saved, and the error says to rerun.
	unreachable := envOf(map[string]string{"CAS_MYSQL_DSN": dsn, "TEMPORAL_ADDRESS": "127.0.0.1:1"})
	if err := run(ctx, []string{"set-limit", "card-1", "250000"}, unreachable); err == nil || !strings.Contains(err.Error(), "run set-limit again") {
		t.Errorf("set-limit with Temporal down: %v, want a rerun hint", err)
	}

	// After a replacement, the new number and the old one both name the account.
	for _, args := range [][]string{{"replace", "card-1", "card-2"}, {"set-apr", "card-2", "1777"}, {"freeze", "card-1"}, {"unfreeze", "card-2"}} {
		if err := run(ctx, args, env); err != nil {
			t.Fatalf("run %q: %v", args, err)
		}
	}
	if err := run(ctx, []string{"replace", "card-1", "card-3"}, env); err == nil {
		t.Error("replaced card-1 twice")
	}
	if status, _, _ := l.CardStatus(ctx, "card-1"); status != CardActive {
		t.Errorf("card-1 status = %q after freeze and unfreeze by its two numbers, want active", status)
	}

	if limit, ok, err := l.CreditLimit(ctx, "card-1"); err != nil || !ok || limit != 250_000 {
		t.Errorf("CreditLimit = %d, %v, %v; want 250000", limit, ok, err)
	}
	if bps, ok, err := l.APR(ctx, "card-1"); err != nil || !ok || bps != 1_777 {
		t.Errorf("APR = %d, %v, %v; want 1777, set through card-1's new number", bps, ok, err)
	}
	if rate, ok, err := l.RewardRate(ctx, "shell"); err != nil || !ok || rate != 4 {
		t.Errorf("RewardRate = %d, %v, %v; want 4", rate, ok, err)
	}
}

// Subscription commands are checked before anything dials Temporal.
func TestSubscriptionArgs(t *testing.T) {
	unreachable := envOf(map[string]string{"TEMPORAL_ADDRESS": "127.0.0.1:1"})
	tests := []struct {
		args    []string
		wantErr string // a valid command gets as far as dialing
	}{
		{[]string{"subscribe", "s1", "card-1", "streamer", "999"}, "dial temporal"},
		{[]string{"unsubscribe", "s1"}, "dial temporal"},
		{[]string{"subscribe", "s1", "card-1", "streamer"}, "usage"},
		{[]string{"subscribe", "", "card-1", "streamer", "999"}, "usage"},
		{[]string{"unsubscribe"}, "usage"},
		{[]string{"unsubscribe", "s1", "now"}, "usage"},
		{[]string{"subscribe", "s1", "card-1", "streamer", "0"}, "not a positive"},
		{[]string{"subscribe", "s1", "card-1", "streamer", "-5"}, "not a positive"},
		{[]string{"subscribe", "s1", "card-1", "streamer", "9.99"}, "not a positive"},
		{[]string{"subscribe", "s1", "card:1", "streamer", "999"}, "can't contain"},
	}
	for _, tt := range tests {
		t.Run(strings.Join(tt.args, " "), func(t *testing.T) {
			if err := run(context.Background(), tt.args, unreachable); err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("run(%q) = %v, want an error containing %q", tt.args, err, tt.wantErr)
			}
		})
	}
}
