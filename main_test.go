package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"task101-leaselock/internal/clock"
	"task101-leaselock/internal/lease"
)

// TestSmokeEntryPoint runs the smoke self-check inside go test so the same
// assertions are exercised by the test suite.
func TestSmokeEntryPoint(t *testing.T) {
	if err := runSmokeTest(); err != nil {
		t.Fatalf("smoke test failed: %v", err)
	}
}

func TestAcquireJSONShape(t *testing.T) {
	clk := clock.NewFakeClock(time.Unix(0, 0))
	ts, _, teardown := newTempService(clk)
	defer teardown()

	body, _ := json.Marshal(acquireReq{Resource: "R", Holder: "H", TTLSeconds: 7})
	resp, err := http.Post(ts.URL+"/leases", "application/json", strings.NewReader(string(body)))
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d want 200", resp.StatusCode)
	}
	var lr leaseResp
	if err := json.NewDecoder(resp.Body).Decode(&lr); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if lr.Token != 1 || lr.TTLSeconds != 7 || lr.Holder != "H" || lr.Resource != "R" {
		t.Fatalf("body = %+v", lr)
	}
}

func TestListEmptyIsJSONArray(t *testing.T) {
	clk := clock.NewFakeClock(time.Unix(0, 0))
	ts, _, teardown := newTempService(clk)
	defer teardown()

	resp, err := http.Get(ts.URL + "/leases")
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	defer resp.Body.Close()
	var lr listResp
	if err := json.NewDecoder(resp.Body).Decode(&lr); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if lr.Leases == nil || len(lr.Leases) != 0 {
		t.Fatalf("leases = %v want []", lr.Leases)
	}
}

func TestHealthzReadyVersion(t *testing.T) {
	clk := clock.NewFakeClock(time.Unix(0, 0))
	ts, _, teardown := newTempService(clk)
	defer teardown()

	for _, p := range []string{"/healthz", "/readyz", "/versionz"} {
		resp, err := http.Get(ts.URL + p)
		if err != nil {
			t.Fatalf("%s: %v", p, err)
		}
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("%s status = %d", p, resp.StatusCode)
		}
		resp.Body.Close()
	}
}

func TestMethodNotAllowed(t *testing.T) {
	clk := clock.NewFakeClock(time.Unix(0, 0))
	ts, _, teardown := newTempService(clk)
	defer teardown()

	req, _ := http.NewRequest(http.MethodPut, ts.URL+"/leases", strings.NewReader(`{}`))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("PUT: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("PUT /leases = %d want 405", resp.StatusCode)
	}
}

func TestUnknownFieldsRejected(t *testing.T) {
	clk := clock.NewFakeClock(time.Unix(0, 0))
	ts, _, teardown := newTempService(clk)
	defer teardown()

	// DisallowUnknownFields is on; an extra field must be a 400.
	resp, err := http.Post(ts.URL+"/leases", "application/json", strings.NewReader(`{"resource":"X","holder":"H","ttl_seconds":5,"bogus":1}`))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("unknown field status = %d want 400", resp.StatusCode)
	}
}

// keep lease referenced for the typed helpers shared with smoke.go.
var _ = lease.Lease{}

// keep httptest referenced (newTempService uses it).
var _ = httptest.NewServer
