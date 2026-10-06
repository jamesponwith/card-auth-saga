package main

import (
	"errors"
	"testing"
)

func TestLedgerPost(t *testing.T) {
	tests := []struct {
		name    string
		entries []Entry
		wantErr error
		wantA   int64 // balance of "a" afterwards
	}{
		{"balanced", []Entry{{"a", 500}, {"b", -500}}, nil, 500},
		{"unbalanced", []Entry{{"a", 500}, {"b", -400}}, ErrUnbalanced, 0},
		{"single leg", []Entry{{"a", 0}}, ErrUnbalanced, 0},
		{"no legs", nil, ErrUnbalanced, 0},
		{"three legs", []Entry{{"a", 300}, {"b", -100}, {"c", -200}}, nil, 300},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := NewLedger()
			if err := l.Post("k", tt.entries...); !errors.Is(err, tt.wantErr) {
				t.Fatalf("Post err = %v, want %v", err, tt.wantErr)
			}
			if got := l.Balance("a"); got != tt.wantA {
				t.Errorf("Balance(a) = %d, want %d", got, tt.wantA)
			}
			if got := l.Total(); got != 0 {
				t.Errorf("Total = %d, want 0", got)
			}
		})
	}
}

func TestLedgerPostIsIdempotent(t *testing.T) {
	l := NewLedger()
	for range 3 {
		if err := l.Post("same-key", Entry{"a", 100}, Entry{"b", -100}); err != nil {
			t.Fatal(err)
		}
	}
	if got := l.Balance("a"); got != 100 {
		t.Errorf("Balance(a) = %d after 3 posts of one key, want 100", got)
	}
}
