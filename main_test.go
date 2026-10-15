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

	if limit, ok, err := l.CreditLimit(ctx, "card-1"); err != nil || !ok || limit != 250_000 {
		t.Errorf("CreditLimit = %d, %v, %v; want 250000", limit, ok, err)
	}
	if bps, ok, err := l.APR(ctx, "card-1"); err != nil || !ok || bps != 1_999 {
		t.Errorf("APR = %d, %v, %v; want 1999", bps, ok, err)
	}
	if rate, ok, err := l.RewardRate(ctx, "shell"); err != nil || !ok || rate != 4 {
		t.Errorf("RewardRate = %d, %v, %v; want 4", rate, ok, err)
	}
}
