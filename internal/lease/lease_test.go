package lease

import (
	"errors"
	"testing"
	"time"
)

func TestLeaseActiveBoundary(t *testing.T) {
	now := time.Unix(100, 0)
	l := Lease{Deadline: now.Add(5 * time.Second)}
	if !l.Active(now) {
		t.Fatal("lease with deadline now+5 should be active at now")
	}
	if l.Active(now.Add(5 * time.Second)) {
		t.Fatal("lease at exact deadline should be inactive")
	}
	if !l.Active(now.Add(5*time.Second - time.Nanosecond)) {
		t.Fatal("lease 1ns before deadline should be active")
	}
}

func TestErrorsAreSentinel(t *testing.T) {
	for _, e := range []error{ErrInvalid, ErrHeld, ErrNotHeld, ErrNotHolder, ErrExpired,
		ErrQuotaExceeded, ErrHolderExists, ErrHolderNotFound, ErrHolderHasLeases,
		ErrPolicyExists, ErrPolicyNotFound, ErrWaiterNotFound, ErrTagNotFound, ErrConflict} {
		if !errors.Is(e, e) {
			t.Fatalf("%v not errors.Is with itself", e)
		}
	}
}

func TestWrappedInvalidIsInvalid(t *testing.T) {
	err := validateResourceHolder("", "H")
	if !IsInvalid(err) {
		t.Fatalf("empty resource = %v want ErrInvalid", err)
	}
	err = validateResourceHolder("X", "")
	if !IsInvalid(err) {
		t.Fatalf("empty holder = %v want ErrInvalid", err)
	}
}

func TestValidateTTL(t *testing.T) {
	cases := []struct {
		ttl   int64
		valid bool
	}{
		{0, false}, {-1, false}, {1, true}, {3600, true}, {3601, false}, {100, true},
	}
	for _, c := range cases {
		err := validateTTL(c.ttl)
		if c.valid && err != nil {
			t.Fatalf("validateTTL(%d) = %v want nil", c.ttl, err)
		}
		if !c.valid && !IsInvalid(err) {
			t.Fatalf("validateTTL(%d) = %v want ErrInvalid", c.ttl, err)
		}
	}
}

func TestValidateTagArgs(t *testing.T) {
	if err := validateTagArgs("", "t"); !IsInvalid(err) {
		t.Fatalf("empty resource = %v want ErrInvalid", err)
	}
	if err := validateTagArgs("r", ""); !IsInvalid(err) {
		t.Fatalf("empty tag = %v want ErrInvalid", err)
	}
	if err := validateTagArgs("r", "t"); err != nil {
		t.Fatalf("valid tag args = %v want nil", err)
	}
}
