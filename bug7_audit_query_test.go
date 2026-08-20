package main

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"task101-leaselock/internal/clock"
	"task101-leaselock/internal/lease"
	"task101-leaselock/internal/store"
)

func TestAuditRejectsMalformedPagination(t *testing.T) {
	st, err := store.Open(t.TempDir() + "/lease.db")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	clk := clock.NewFakeClock(time.Unix(700, 0))
	svc := lease.NewService(st, clk)
	if _, err := svc.Acquire("X", "H", 30, ""); err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodGet, "/audit?limit=not-a-number", nil)
	rec := httptest.NewRecorder()
	newMux(svc).ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("malformed audit pagination status=%d body=%s", rec.Code, rec.Body.String())
	}
}
