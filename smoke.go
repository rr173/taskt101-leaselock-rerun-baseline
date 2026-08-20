package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"time"

	"task101-leaselock/internal/clock"
	"task101-leaselock/internal/lease"
)

type sm struct {
	name string
	err  error
}

// ---- HTTP helpers -----------------------------------------------------------

func postJSON(ts *httptest.Server, path string, body any) (*http.Response, error) {
	raw, _ := json.Marshal(body)
	resp, err := http.Post(ts.URL+path, "application/json", bytes.NewReader(raw))
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func postRaw(ts *httptest.Server, path string, raw []byte) (int, errResp, error) {
	resp, err := http.Post(ts.URL+path, "application/json", bytes.NewReader(raw))
	if err != nil {
		return 0, errResp{}, err
	}
	defer resp.Body.Close()
	var e errResp
	if resp.StatusCode != http.StatusOK {
		_ = json.NewDecoder(resp.Body).Decode(&e)
	}
	return resp.StatusCode, e, nil
}

func doMethod(ts *httptest.Server, method, path string) (*http.Response, error) {
	req, _ := http.NewRequest(method, ts.URL+path, nil)
	return http.DefaultClient.Do(req)
}

func decodeBody[T any](resp *http.Response) (T, error) {
	var v T
	err := json.NewDecoder(resp.Body).Decode(&v)
	return v, err
}

func acquireOK(_ *httptest.Server, _ string, _ string, _ int64) leaseResp {
	return leaseResp{}
}

// smoke-scoped acquire that records into the scenario results instead of
// failing the test directly.
func sAcquire(rec func(string, error), name string, ts *httptest.Server, resource, holder string, ttl int64) leaseResp {
	resp, err := postJSON(ts, "/leases", acquireReq{Resource: resource, Holder: holder, TTLSeconds: ttl})
	if err != nil {
		rec(name, err)
		return leaseResp{}
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		rec(name, fmt.Errorf("acquire %s: status %d", resource, resp.StatusCode))
		return leaseResp{}
	}
	lr, _ := decodeBody[leaseResp](resp)
	return lr
}

// newTempService builds a service over a fresh bbolt file and returns the test
// server plus teardown.
func newTempService(clk clock.Clock) (*httptest.Server, *Service, func()) {
	dir, err := os.MkdirTemp("", "leaselock-smoke-*")
	if err != nil {
		panic(err)
	}
	svc, err := newService(filepath.Join(dir, "leases.db"), clk)
	if err != nil {
		os.RemoveAll(dir)
		panic(err)
	}
	if _, err := svc.svc.Recover(); err != nil {
		svc.close()
		os.RemoveAll(dir)
		panic(err)
	}
	ts := httptest.NewServer(newMux(svc.svc))
	teardown := func() {
		ts.Close()
		svc.close()
		os.RemoveAll(dir)
	}
	return ts, svc, teardown
}

// runSmokeTest exercises the full service through HTTP with isolated state per
// scenario. Returns a non-nil error if any assertion fails.
func runSmokeTest() error {
	var (
		results []sm
		mu      sync.Mutex
	)
	record := func(name string, err error) {
		mu.Lock()
		results = append(results, sm{name: name, err: err})
		mu.Unlock()
	}

	// 1. Acquire -> renew -> release happy path.
	func() {
		clk := clock.NewFakeClock(time.Unix(1_000_000, 0))
		ts, _, teardown := newTempService(clk)
		defer teardown()

		lr := sAcquire(record, "happy", ts, "X", "H1", 5)
		if lr.Token != 1 || lr.DeadlineUnix != 1_000_000+5 {
			record("happy", fmt.Errorf("acquire = %+v", lr))
			return
		}
		rr, code, err := postRenew(ts, "X", "H1", 1, 5)
		if err != nil || code != 200 || rr.DeadlineUnix != 1_000_000+5 {
			record("happy", fmt.Errorf("renew: code=%d rr=%+v err=%v", code, rr, err))
			return
		}
		clk.Advance(3 * time.Second)
		rr, code, err = postRenew(ts, "X", "H1", 1, 5)
		if err != nil || code != 200 || rr.DeadlineUnix != 1_000_003+5 {
			record("happy", fmt.Errorf("renew after advance: code=%d rr=%+v err=%v", code, rr, err))
			return
		}
		rel, code, err := postRelease(ts, "X", "H1", 1)
		if err != nil || code != 200 || !rel.Released {
			record("happy", fmt.Errorf("release: code=%d rel=%+v err=%v", code, rel, err))
			return
		}
		record("happy", nil)
	}()

	// 2. Acquiring a held resource returns 409.
	func() {
		clk := clock.NewFakeClock(time.Unix(2_000_000, 0))
		ts, _, teardown := newTempService(clk)
		defer teardown()
		sAcquire(record, "held", ts, "X", "H1", 10)
		if code, _, _ := postRaw(ts, "/leases", bodyJSON(acquireReq{Resource: "X", Holder: "H2", TTLSeconds: 5})); code != 409 {
			record("held", fmt.Errorf("second acquire code=%d want 409", code))
			return
		}
		record("held", nil)
	}()

	// 3. Renewing/releasing an expired lease returns 410.
	func() {
		clk := clock.NewFakeClock(time.Unix(3_000_000, 0))
		ts, _, teardown := newTempService(clk)
		defer teardown()
		sAcquire(record, "expired", ts, "X", "H1", 5)
		clk.Advance(6 * time.Second)
		if _, code, _ := postRenew(ts, "X", "H1", 1, 5); code != 410 {
			record("expired", fmt.Errorf("renew expired code=%d want 410", code))
			return
		}
		if _, code, _ := postRelease(ts, "X", "H1", 1); code != 410 {
			record("expired", fmt.Errorf("release expired code=%d want 410", code))
			return
		}
		record("expired", nil)
	}()

	// 4. Stale token cannot renew/release a re-acquired lease (409).
	func() {
		clk := clock.NewFakeClock(time.Unix(4_000_000, 0))
		ts, _, teardown := newTempService(clk)
		defer teardown()
		lr1 := sAcquire(record, "stale", ts, "X", "H1", 5)
		clk.Advance(6 * time.Second)
		lr2, code, _ := postAcquire(ts, "X", "H2", 5)
		if code != 200 || lr2.Token <= lr1.Token {
			record("stale", fmt.Errorf("re-acquire code=%d token=%d want >%d", code, lr2.Token, lr1.Token))
			return
		}
		if _, code, _ := postRenew(ts, "X", "H1", lr1.Token, 5); code != 409 {
			record("stale", fmt.Errorf("stale renew code=%d want 409", code))
			return
		}
		if _, code, _ := postRelease(ts, "X", "H1", lr1.Token); code != 409 {
			record("stale", fmt.Errorf("stale release code=%d want 409", code))
			return
		}
		record("stale", nil)
	}()

	// 5. List sorted by resource with active flags.
	func() {
		clk := clock.NewFakeClock(time.Unix(5_000_000, 0))
		ts, _, teardown := newTempService(clk)
		defer teardown()
		sAcquire(record, "list", ts, "C", "H", 5)
		sAcquire(record, "list", ts, "A", "H", 5)
		sAcquire(record, "list", ts, "B", "H", 100)
		lr, code, err := getList(ts)
		if err != nil || code != 200 {
			record("list", fmt.Errorf("list code=%d err=%v", code, err))
			return
		}
		got := []string{lr.Leases[0].Resource, lr.Leases[1].Resource, lr.Leases[2].Resource}
		if !reflect.DeepEqual(got, []string{"A", "B", "C"}) {
			record("list", fmt.Errorf("order=%v want [A B C]", got))
			return
		}
		clk.Advance(6 * time.Second)
		lr, _, _ = getList(ts)
		active := map[string]bool{}
		for _, l := range lr.Leases {
			active[l.Resource] = l.Active != nil && *l.Active
		}
		if active["A"] || active["C"] || !active["B"] {
			record("list", fmt.Errorf("active=%v", active))
			return
		}
		record("list", nil)
	}()

	// 6. Expire sweep count.
	func() {
		clk := clock.NewFakeClock(time.Unix(6_000_000, 0))
		ts, _, teardown := newTempService(clk)
		defer teardown()
		sAcquire(record, "expire", ts, "A", "H", 5)
		sAcquire(record, "expire", ts, "B", "H", 100)
		sAcquire(record, "expire", ts, "C", "H", 5)
		if er, _, _ := postExpire(ts); er.ExpiredCount != 0 {
			record("expire", fmt.Errorf("pre count=%d want 0", er.ExpiredCount))
			return
		}
		clk.Advance(6 * time.Second)
		if er, _, _ := postExpire(ts); er.ExpiredCount != 2 {
			record("expire", fmt.Errorf("post count=%d want 2", er.ExpiredCount))
			return
		}
		record("expire", nil)
	}()

	// 7. Transfer keeps token, changes holder.
	func() {
		clk := clock.NewFakeClock(time.Unix(7_000_000, 0))
		ts, _, teardown := newTempService(clk)
		defer teardown()
		lr := sAcquire(record, "transfer", ts, "X", "H1", 50)
		tr, code, err := postTransfer(ts, "X", "H1", lr.Token, "H2")
		if err != nil || code != 200 || tr.Token != lr.Token || tr.Holder != "H2" {
			record("transfer", fmt.Errorf("transfer code=%d tr=%+v want token=%d holder=H2", code, tr, lr.Token))
			return
		}
		// Old holder cannot release.
		if _, code, _ := postRelease(ts, "X", "H1", lr.Token); code != 409 {
			record("transfer", fmt.Errorf("old holder release code=%d want 409", code))
			return
		}
		// New holder can release.
		if rel, code, _ := postRelease(ts, "X", "H2", lr.Token); code != 200 || !rel.Released {
			record("transfer", fmt.Errorf("new holder release code=%d", code))
			return
		}
		record("transfer", nil)
	}()

	// 8. Bulk acquire all-or-nothing.
	func() {
		clk := clock.NewFakeClock(time.Unix(8_000_000, 0))
		ts, _, teardown := newTempService(clk)
		defer teardown()
		sAcquire(record, "bulk", ts, "Y", "H", 5) // Y already held
		if code, _, _ := postRaw(ts, "/leases/bulk-acquire", bodyJSON(bulkAcquireReq{Holder: "H", TTLSeconds: 5, Resources: []string{"X", "Y", "Z"}})); code != 409 {
			record("bulk", fmt.Errorf("bulk with held member code=%d want 409", code))
			return
		}
		// X and Z should not be held (atomic rollback).
		if lr, code, _ := getInspect(ts, "X"); code != 404 {
			record("bulk", fmt.Errorf("X after failed bulk code=%d want 404 (got %+v)", code, lr))
			return
		}
		// Now a clean bulk succeeds.
		bar, code, err := postBulkAcquire(ts, "H", []string{"X", "Z"}, 5)
		if err != nil || code != 200 || len(bar.Granted) != 2 {
			record("bulk", fmt.Errorf("clean bulk code=%d granted=%d err=%v", code, len(bar.Granted), err))
			return
		}
		if bar.Granted[0].Token >= bar.Granted[1].Token {
			record("bulk", fmt.Errorf("bulk tokens not increasing: %d %d", bar.Granted[0].Token, bar.Granted[1].Token))
			return
		}
		record("bulk", nil)
	}()

	// 9. Bulk release all-or-nothing.
	func() {
		clk := clock.NewFakeClock(time.Unix(9_000_000, 0))
		ts, _, teardown := newTempService(clk)
		defer teardown()
		bar, code, _ := postBulkAcquire(ts, "H", []string{"A", "B"}, 50)
		if code != 200 {
			record("bulkrelease", fmt.Errorf("setup bulk acquire code=%d", code))
			return
		}
		// One entry with wrong token -> whole batch fails.
		entries := []bulkReleaseEntry{
			{Resource: "A", Token: bar.Granted[0].Token},
			{Resource: "B", Token: 99999},
		}
		if code, _, _ := postRaw(ts, "/leases/bulk-release", bodyJSON(bulkReleaseReq{Holder: "H", Entries: entries})); code != 409 {
			record("bulkrelease", fmt.Errorf("bad bulk release code=%d want 409", code))
			return
		}
		// A still held.
		if _, code, _ := getInspect(ts, "A"); code != 200 {
			record("bulkrelease", fmt.Errorf("A code=%d want 200 (should still be held)", code))
			return
		}
		// Clean bulk release.
		entries = []bulkReleaseEntry{
			{Resource: "A", Token: bar.Granted[0].Token},
			{Resource: "B", Token: bar.Granted[1].Token},
		}
		br, code, _ := postBulkRelease(ts, "H", entries)
		if code != 200 || br.Released != 2 {
			record("bulkrelease", fmt.Errorf("clean bulk release code=%d released=%d want 2", code, br.Released))
			return
		}
		record("bulkrelease", nil)
	}()

	// 10. Holders + quota enforcement.
	func() {
		clk := clock.NewFakeClock(time.Unix(10_000_000, 0))
		ts, _, teardown := newTempService(clk)
		defer teardown()
		// Register holder with max_concurrent=2.
		if code, _, _ := postRaw(ts, "/holders", bodyJSON(holderReq{ID: "H1", Description: "d", MaxConcurrent: 2})); code != 200 {
			record("quota", fmt.Errorf("register holder code=%d", code))
			return
		}
		sAcquire(record, "quota", ts, "A", "H1", 5)
		sAcquire(record, "quota", ts, "B", "H1", 5)
		// Third acquire exceeds quota -> 429.
		if code, _, _ := postRaw(ts, "/leases", bodyJSON(acquireReq{Resource: "C", Holder: "H1", TTLSeconds: 5})); code != 429 {
			record("quota", fmt.Errorf("over-quota acquire code=%d want 429", code))
			return
		}
		// Release one, then acquire succeeds.
		if _, code, _ := postRelease(ts, "A", "H1", 1); code != 200 {
			record("quota", fmt.Errorf("release A code=%d", code))
			return
		}
		if code, _, _ := postRaw(ts, "/leases", bodyJSON(acquireReq{Resource: "C", Holder: "H1", TTLSeconds: 5})); code != 200 {
			record("quota", fmt.Errorf("post-release acquire code=%d want 200", code))
			return
		}
		// Delete holder with active leases -> 409.
		if code, _, _ := postRaw(ts, "/leases", bodyJSON(acquireReq{Resource: "A", Holder: "H1", TTLSeconds: 5})); code != 200 {
			// re-acquire A to make H1 hold 2 again
		}
		_, _, _ = postRaw(ts, "/leases", bodyJSON(acquireReq{Resource: "A", Holder: "H1", TTLSeconds: 5}))
		if resp, err := doMethod(ts, http.MethodDelete, "/holders/H1"); err != nil || resp.StatusCode != 409 {
			record("quota", fmt.Errorf("delete holder with leases code=%v want 409", resp.StatusCode))
			return
		} else {
			resp.Body.Close()
		}
		record("quota", nil)
	}()

	// 11. Policies referenced by acquire.
	func() {
		clk := clock.NewFakeClock(time.Unix(11_000_000, 0))
		ts, _, teardown := newTempService(clk)
		defer teardown()
		if code, _, _ := postRaw(ts, "/policies", bodyJSON(policyReq{Name: "short", TTLSeconds: 10, Description: "d"})); code != 200 {
			record("policy", fmt.Errorf("create policy code=%d", code))
			return
		}
		// Acquire with policy.
		lr, code, err := postAcquirePolicy(ts, "X", "H", "short")
		if err != nil || code != 200 || lr.TTLSeconds != 10 {
			record("policy", fmt.Errorf("acquire by policy code=%d lr=%+v", code, lr))
			return
		}
		// Both ttl_seconds and policy -> 400.
		if code, _, _ := postRaw(ts, "/leases", bodyJSON(acquireReq{Resource: "Y", Holder: "H", TTLSeconds: 5, Policy: "short"})); code != 400 {
			record("policy", fmt.Errorf("both ttl+policy code=%d want 400", code))
			return
		}
		// Unknown policy -> 404.
		if code, _, _ := postRaw(ts, "/leases", bodyJSON(acquireReq{Resource: "Z", Holder: "H", Policy: "nope"})); code != 404 {
			record("policy", fmt.Errorf("unknown policy code=%d want 404", code))
			return
		}
		record("policy", nil)
	}()

	// 12. Waiters: queue + promotion on release.
	func() {
		clk := clock.NewFakeClock(time.Unix(12_000_000, 0))
		ts, _, teardown := newTempService(clk)
		defer teardown()
		lr := sAcquire(record, "waiter", ts, "X", "H1", 50)
		// Queue a waiter (resource held) -> 202 pending.
		w1, code, _ := postWait(ts, "X", "H2", 5)
		if code != 202 || w1.Status != lease.WaiterPending {
			record("waiter", fmt.Errorf("queue waiter code=%d status=%v want 202 pending", code, w1.Status))
			return
		}
		// Release X -> waiter promoted.
		if _, code, _ := postRelease(ts, "X", "H1", lr.Token); code != 200 {
			record("waiter", fmt.Errorf("release code=%d", code))
			return
		}
		// Now H2 holds X with a new token > lr.Token.
		got, code, _ := getInspect(ts, "X")
		if code != 200 || got.Holder != "H2" || got.Token <= lr.Token {
			record("waiter", fmt.Errorf("after release X=%+v want H2 token>%d", got, lr.Token))
			return
		}
		// Waiter status now granted.
		ws, code, _ := getWaiters(ts, "X", "")
		if code != 200 || len(ws.Waiters) != 1 || ws.Waiters[0].Status != lease.WaiterGranted {
			record("waiter", fmt.Errorf("waiters=%+v want 1 granted", ws))
			return
		}
		// Enqueue on a free resource grants immediately (200).
		w2, code, _ := postWait(ts, "FREE", "H3", 5)
		if code != 200 || w2.Status != lease.WaiterGranted {
			record("waiter", fmt.Errorf("immediate grant code=%d status=%v want 200 granted", code, w2.Status))
			return
		}
		record("waiter", nil)
	}()

	// 13. Tags: add, list by tag, remove.
	func() {
		clk := clock.NewFakeClock(time.Unix(13_000_000, 0))
		ts, _, teardown := newTempService(clk)
		defer teardown()
		addTag := func(r, t string) {
			postRaw(ts, "/resources/"+r+"/tags", bodyJSON(tagReq{Tag: t}))
		}
		addTag("A", "prod")
		addTag("A", "web")
		addTag("B", "prod")
		// List by tag=prod -> [A B].
		rr, code, _ := getResources(ts, "prod")
		if code != 200 || !reflect.DeepEqual(rr.Resources, []string{"A", "B"}) {
			record("tags", fmt.Errorf("tag=prod resources=%v want [A B]", rr.Resources))
			return
		}
		// GetTags(A) -> [prod web].
		tags, _ := getTags(ts, "A")
		if !reflect.DeepEqual(tags, []string{"prod", "web"}) {
			record("tags", fmt.Errorf("A tags=%v want [prod web]", tags))
			return
		}
		// Remove tag web from A.
		resp, err := doMethod(ts, http.MethodDelete, "/resources/A/tags/web")
		if err != nil || resp.StatusCode != 200 {
			record("tags", fmt.Errorf("remove tag code=%v", resp.StatusCode))
			return
		}
		resp.Body.Close()
		tags, _ = getTags(ts, "A")
		if !reflect.DeepEqual(tags, []string{"prod"}) {
			record("tags", fmt.Errorf("A tags after remove=%v want [prod]", tags))
			return
		}
		// Remove non-existent tag -> 404.
		if resp, err := doMethod(ts, http.MethodDelete, "/resources/A/tags/nope"); err != nil || resp.StatusCode != 404 {
			record("tags", fmt.Errorf("remove missing tag code=%v want 404", resp.StatusCode))
			return
		} else {
			resp.Body.Close()
		}
		record("tags", nil)
	}()

	// 14. Audit log + token history.
	func() {
		clk := clock.NewFakeClock(time.Unix(14_000_000, 0))
		ts, _, teardown := newTempService(clk)
		defer teardown()
		lr := sAcquire(record, "audit", ts, "X", "H1", 5)
		postRenew(ts, "X", "H1", lr.Token, 5)
		// Audit entries for X: acquire + renew.
		ar, code, _ := getAudit(ts, "X")
		if code != 200 || len(ar.Entries) != 2 {
			record("audit", fmt.Errorf("audit entries=%d want 2", len(ar.Entries)))
			return
		}
		// Token history for X: acquire + renew (both carry tokens).
		th, code, _ := getTokenHistory(ts, "X")
		if code != 200 || len(th.Entries) != 2 {
			record("audit", fmt.Errorf("token history=%d want 2", len(th.Entries)))
			return
		}
		record("audit", nil)
	}()

	// 15. Stats.
	func() {
		clk := clock.NewFakeClock(time.Unix(15_000_000, 0))
		ts, _, teardown := newTempService(clk)
		defer teardown()
		sAcquire(record, "stats", ts, "A", "H", 5)
		sAcquire(record, "stats", ts, "B", "H", 100)
		st, code, err := getStats(ts)
		if err != nil || code != 200 {
			record("stats", fmt.Errorf("stats code=%d err=%v", code, err))
			return
		}
		if st.TotalLeases != 2 || st.ActiveLeases != 2 || st.NextFencingToken != 3 {
			record("stats", fmt.Errorf("stats=%+v want total=2 active=2 next=3", st))
			return
		}
		record("stats", nil)
	}()

	// 16. Restart recovery: leases + fencing survive; downtime-expired reaped.
	func() {
		dir, err := os.MkdirTemp("", "leaselock-restart-*")
		if err != nil {
			record("restart", err)
			return
		}
		defer os.RemoveAll(dir)
		dbPath := filepath.Join(dir, "leases.db")

		c1 := clock.NewFakeClock(time.Unix(16_000_000, 0))
		svc1, err := newService(dbPath, c1)
		if err != nil {
			record("restart", err)
			return
		}
		ts1 := httptest.NewServer(newMux(svc1.svc))
		lx := sAcquire(record, "restart", ts1, "X", "H1", 5)
		ly := sAcquire(record, "restart", ts1, "Y", "H2", 100)
		ts1.Close()
		if err := svc1.close(); err != nil {
			record("restart", err)
			return
		}
		_ = lx

		c2 := clock.NewFakeClock(time.Unix(16_000_010, 0)) // +10s: X expired, Y alive
		svc2, err := newService(dbPath, c2)
		if err != nil {
			record("restart", err)
			return
		}
		defer svc2.close()
		n, err := svc2.svc.Recover()
		if err != nil || n != 1 {
			record("restart", fmt.Errorf("recover reaped=%d want 1", n))
			return
		}
		ts2 := httptest.NewServer(newMux(svc2.svc))
		defer ts2.Close()

		yi, code, _ := getInspect(ts2, "Y")
		if code != 200 || yi.Token != ly.Token || yi.Active == nil || !*yi.Active {
			record("restart", fmt.Errorf("Y=%+v want token=%d active", yi, ly.Token))
			return
		}
		// X re-acquired with strictly larger token.
		x2, code, _ := postAcquire(ts2, "X", "H3", 5)
		if code != 200 || x2.Token <= ly.Token {
			record("restart", fmt.Errorf("X re-token=%d want >%d", x2.Token, ly.Token))
			return
		}
		record("restart", nil)
	}()

	// 17. Validation: empty fields, bad TTL.
	func() {
		clk := clock.NewFakeClock(time.Unix(17_000_000, 0))
		ts, _, teardown := newTempService(clk)
		defer teardown()
		cases := []struct{ name, body string }{
			{"empty-resource", `{"resource":"","holder":"H","ttl_seconds":5}`},
			{"empty-holder", `{"resource":"X","holder":"","ttl_seconds":5}`},
			{"ttl-zero", `{"resource":"X","holder":"H","ttl_seconds":0}`},
			{"ttl-big", `{"resource":"X","holder":"H","ttl_seconds":3601}`},
		}
		for _, c := range cases {
			if code, e, _ := postRaw(ts, "/leases", []byte(c.body)); code != 400 || e.Error == "" {
				record("validation:"+c.name, fmt.Errorf("code=%d err=%q want 400", code, e.Error))
				return
			}
		}
		record("validation", nil)
	}()

	// 18. healthz / readyz / versionz + method-not-allowed.
	func() {
		clk := clock.NewFakeClock(time.Unix(18_000_000, 0))
		ts, _, teardown := newTempService(clk)
		defer teardown()
		for _, p := range []string{"/healthz", "/readyz", "/versionz"} {
			resp, err := http.Get(ts.URL + p)
			if err != nil || resp.StatusCode != 200 {
				record("meta", fmt.Errorf("%s code=%v", p, resp.StatusCode))
				return
			}
			resp.Body.Close()
		}
		if resp, err := doMethod(ts, http.MethodPut, "/leases"); err != nil || resp.StatusCode != 405 {
			record("meta", fmt.Errorf("PUT /leases code=%v want 405", resp.StatusCode))
			return
		} else {
			resp.Body.Close()
		}
		record("meta", nil)
	}()

	var failed int
	for _, r := range results {
		if r.err != nil {
			failed++
			fmt.Printf("  - %s: %v\n", r.name, r.err)
		}
	}
	if failed > 0 {
		return errors.New("smoke test assertions failed")
	}
	return nil
}

// bodyJSON marshals v to bytes (panic-safe for tests).
func bodyJSON(v any) []byte {
	raw, _ := json.Marshal(v)
	return raw
}

// ---- typed HTTP call helpers -----------------------------------------------

func postAcquire(ts *httptest.Server, resource, holder string, ttl int64) (leaseResp, int, error) {
	resp, err := postJSON(ts, "/leases", acquireReq{Resource: resource, Holder: holder, TTLSeconds: ttl})
	if err != nil {
		return leaseResp{}, 0, err
	}
	defer resp.Body.Close()
	var lr leaseResp
	if resp.StatusCode == 200 {
		lr, _ = decodeBody[leaseResp](resp)
	}
	return lr, resp.StatusCode, nil
}

func postAcquirePolicy(ts *httptest.Server, resource, holder, policy string) (leaseResp, int, error) {
	resp, err := postJSON(ts, "/leases", acquireReq{Resource: resource, Holder: holder, Policy: policy})
	if err != nil {
		return leaseResp{}, 0, err
	}
	defer resp.Body.Close()
	var lr leaseResp
	if resp.StatusCode == 200 {
		lr, _ = decodeBody[leaseResp](resp)
	}
	return lr, resp.StatusCode, nil
}

func postRenew(ts *httptest.Server, resource, holder string, token, ttl int64) (renewResp, int, error) {
	resp, err := postJSON(ts, "/leases/renew", renewReq{Resource: resource, Holder: holder, Token: token, TTLSeconds: ttl})
	if err != nil {
		return renewResp{}, 0, err
	}
	defer resp.Body.Close()
	var rr renewResp
	if resp.StatusCode == 200 {
		rr, _ = decodeBody[renewResp](resp)
	}
	return rr, resp.StatusCode, nil
}

func postRelease(ts *httptest.Server, resource, holder string, token int64) (releaseResp, int, error) {
	resp, err := postJSON(ts, "/leases/release", releaseReq{Resource: resource, Holder: holder, Token: token})
	if err != nil {
		return releaseResp{}, 0, err
	}
	defer resp.Body.Close()
	var rr releaseResp
	if resp.StatusCode == 200 {
		rr, _ = decodeBody[releaseResp](resp)
	}
	return rr, resp.StatusCode, nil
}

func postTransfer(ts *httptest.Server, resource, holder string, token int64, newHolder string) (renewResp, int, error) {
	resp, err := postJSON(ts, "/leases/transfer", transferReq{Resource: resource, Holder: holder, Token: token, NewHolder: newHolder})
	if err != nil {
		return renewResp{}, 0, err
	}
	defer resp.Body.Close()
	var rr renewResp
	if resp.StatusCode == 200 {
		rr, _ = decodeBody[renewResp](resp)
	}
	return rr, resp.StatusCode, nil
}

func postBulkAcquire(ts *httptest.Server, holder string, resources []string, ttl int64) (bulkAcquireResp, int, error) {
	resp, err := postJSON(ts, "/leases/bulk-acquire", bulkAcquireReq{Holder: holder, TTLSeconds: ttl, Resources: resources})
	if err != nil {
		return bulkAcquireResp{}, 0, err
	}
	defer resp.Body.Close()
	var bar bulkAcquireResp
	if resp.StatusCode == 200 {
		bar, _ = decodeBody[bulkAcquireResp](resp)
	}
	return bar, resp.StatusCode, nil
}

func postBulkRelease(ts *httptest.Server, holder string, entries []bulkReleaseEntry) (bulkReleaseResp, int, error) {
	resp, err := postJSON(ts, "/leases/bulk-release", bulkReleaseReq{Holder: holder, Entries: entries})
	if err != nil {
		return bulkReleaseResp{}, 0, err
	}
	defer resp.Body.Close()
	var br bulkReleaseResp
	if resp.StatusCode == 200 {
		br, _ = decodeBody[bulkReleaseResp](resp)
	}
	return br, resp.StatusCode, nil
}

func postExpire(ts *httptest.Server) (expireResp, int, error) {
	resp, err := postJSON(ts, "/leases/expire", nil)
	if err != nil {
		return expireResp{}, 0, err
	}
	defer resp.Body.Close()
	var er expireResp
	if resp.StatusCode == 200 {
		er, _ = decodeBody[expireResp](resp)
	}
	return er, resp.StatusCode, nil
}

func postWait(ts *httptest.Server, resource, holder string, ttl int64) (waiterResp, int, error) {
	resp, err := postJSON(ts, "/leases/wait", waitReq{Resource: resource, Holder: holder, TTLSeconds: ttl})
	if err != nil {
		return waiterResp{}, 0, err
	}
	defer resp.Body.Close()
	var w waiterResp
	if resp.StatusCode == 200 || resp.StatusCode == 202 {
		w, _ = decodeBody[waiterResp](resp)
	}
	return w, resp.StatusCode, nil
}

func getList(ts *httptest.Server) (listResp, int, error) {
	resp, err := http.Get(ts.URL + "/leases")
	if err != nil {
		return listResp{}, 0, err
	}
	defer resp.Body.Close()
	var lr listResp
	if resp.StatusCode == 200 {
		lr, _ = decodeBody[listResp](resp)
	}
	return lr, resp.StatusCode, nil
}

func getInspect(ts *httptest.Server, resource string) (leaseResp, int, error) {
	resp, err := http.Get(ts.URL + "/leases?resource=" + resource)
	if err != nil {
		return leaseResp{}, 0, err
	}
	defer resp.Body.Close()
	var lr leaseResp
	if resp.StatusCode == 200 {
		lr, _ = decodeBody[leaseResp](resp)
	}
	return lr, resp.StatusCode, nil
}

func getWaiters(ts *httptest.Server, resource, status string) (listWaitersResp, int, error) {
	q := "/leases/waiters"
	if resource != "" || status != "" {
		q += "?"
		if resource != "" {
			q += "resource=" + resource + "&"
		}
		if status != "" {
			q += "status=" + status
		}
	}
	resp, err := http.Get(ts.URL + q)
	if err != nil {
		return listWaitersResp{}, 0, err
	}
	defer resp.Body.Close()
	var lr listWaitersResp
	if resp.StatusCode == 200 {
		lr, _ = decodeBody[listWaitersResp](resp)
	}
	return lr, resp.StatusCode, nil
}

func getResources(ts *httptest.Server, tag string) (listResourcesResp, int, error) {
	q := "/resources"
	if tag != "" {
		q += "?tag=" + tag
	}
	resp, err := http.Get(ts.URL + q)
	if err != nil {
		return listResourcesResp{}, 0, err
	}
	defer resp.Body.Close()
	var lr listResourcesResp
	if resp.StatusCode == 200 {
		lr, _ = decodeBody[listResourcesResp](resp)
	}
	return lr, resp.StatusCode, nil
}

func getTags(ts *httptest.Server, resource string) ([]string, error) {
	resp, err := http.Get(ts.URL + "/resources/" + resource + "/tags")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var tr tagsResp
	if resp.StatusCode == 200 {
		tr, _ = decodeBody[tagsResp](resp)
	}
	return tr.Tags, nil
}

func getAudit(ts *httptest.Server, resource string) (listAuditResp, int, error) {
	resp, err := http.Get(ts.URL + "/audit?resource=" + resource)
	if err != nil {
		return listAuditResp{}, 0, err
	}
	defer resp.Body.Close()
	var ar listAuditResp
	if resp.StatusCode == 200 {
		ar, _ = decodeBody[listAuditResp](resp)
	}
	return ar, resp.StatusCode, nil
}

func getTokenHistory(ts *httptest.Server, resource string) (listAuditResp, int, error) {
	resp, err := http.Get(ts.URL + "/audit/tokens?resource=" + resource)
	if err != nil {
		return listAuditResp{}, 0, err
	}
	defer resp.Body.Close()
	var ar listAuditResp
	if resp.StatusCode == 200 {
		ar, _ = decodeBody[listAuditResp](resp)
	}
	return ar, resp.StatusCode, nil
}

func getStats(ts *httptest.Server) (lease.Stats, int, error) {
	resp, err := http.Get(ts.URL + "/stats")
	if err != nil {
		return lease.Stats{}, 0, err
	}
	defer resp.Body.Close()
	var st lease.Stats
	if resp.StatusCode == 200 {
		st, _ = decodeBody[lease.Stats](resp)
	}
	return st, resp.StatusCode, nil
}
