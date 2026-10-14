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

// The admin commands write the config the activities read.
func TestAdminCommands(t *testing.T) {
	dsn := freshDB(t)
	ctx := context.Background()
	for _, args := range [][]string{{"set-limit", "card-1", "250000"}, {"set-rate", "shell", "4"}} {
		if err := run(ctx, args, dsn); err != nil {
			t.Fatalf("run %q: %v", args, err)
		}
	}
	if err := run(ctx, []string{"set-limit", "card-1", "-1"}, dsn); err == nil {
		t.Error("negative limit accepted")
	}
	if err := run(ctx, []string{"set-limit", "card-1", "1"}, ""); err == nil || !strings.Contains(err.Error(), "CAS_MYSQL_DSN") {
		t.Errorf("without a DSN: %v, want a pointer to CAS_MYSQL_DSN", err)
	}

	l := open(t, dsn)
	if limit, ok, err := l.CreditLimit(ctx, "card-1"); err != nil || !ok || limit != 250_000 {
		t.Errorf("CreditLimit = %d, %v, %v; want 250000", limit, ok, err)
	}
	if rate, ok, err := l.RewardRate(ctx, "shell"); err != nil || !ok || rate != 4 {
		t.Errorf("RewardRate = %d, %v, %v; want 4", rate, ok, err)
	}
}
