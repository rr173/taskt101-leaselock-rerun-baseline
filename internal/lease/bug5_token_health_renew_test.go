package lease_test

import (
	"testing"
	"time"

	"task101-leaselock/internal/clock"
	"task101-leaselock/internal/lease"
	"task101-leaselock/internal/store"
)

func TestTokenHealthAllowsRepeatedUseOfSameToken(t *testing.T) {
	clk := clock.NewFakeClock(time.Unix(500, 0))
	st, err := store.Open(t.TempDir() + "/lease.db")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	svc := lease.NewService(st, clk)
	l, err := svc.Acquire("X", "H", 30, "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Renew("X", "H", l.Token, 30); err != nil {
		t.Fatal(err)
	}
	health, err := svc.CheckTokenHealth("X")
	if err != nil {
		t.Fatal(err)
	}
	if !health.StrictlyIncreasing || health.Entries != 2 || health.LastToken != l.Token {
		t.Fatalf("token health=%+v want repeated token to remain healthy", health)
	}
}
