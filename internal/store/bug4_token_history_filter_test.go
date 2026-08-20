package store

import (
	"testing"
	"time"
)

func TestTokenHistoryFiltersByResource(t *testing.T) {
	s, _ := newTestStore(t)
	defer s.Close()
	now := time.Unix(400, 0)
	s.Acquire("X", "HX", now, time.Minute)
	s.Acquire("Y", "HY", now, time.Minute)
	entries, err := s.TokenHistory("X")
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Resource != "X" {
		t.Fatalf("token history for X=%+v", entries)
	}
}
