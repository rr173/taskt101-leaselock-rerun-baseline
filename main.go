package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"

	"task101-leaselock/internal/clock"
	"task101-leaselock/internal/lease"
	"task101-leaselock/internal/store"
)

// ----------------------------------------------------------------------------
// Request / response shapes
// ----------------------------------------------------------------------------

type acquireReq struct {
	Resource   string `json:"resource"`
	Holder     string `json:"holder"`
	TTLSeconds int64  `json:"ttl_seconds"`
	Policy     string `json:"policy"`
}

type renewReq struct {
	Resource   string `json:"resource"`
	Holder     string `json:"holder"`
	Token      int64  `json:"token"`
	TTLSeconds int64  `json:"ttl_seconds"`
}

type releaseReq struct {
	Resource string `json:"resource"`
	Holder   string `json:"holder"`
	Token    int64  `json:"token"`
}

type transferReq struct {
	Resource  string `json:"resource"`
	Holder    string `json:"holder"`
	Token     int64  `json:"token"`
	NewHolder string `json:"new_holder"`
}

type bulkAcquireReq struct {
	Holder     string   `json:"holder"`
	TTLSeconds int64    `json:"ttl_seconds"`
	Resources  []string `json:"resources"`
}

type bulkReleaseEntry struct {
	Resource string `json:"resource"`
	Token    int64  `json:"token"`
}

type bulkReleaseReq struct {
	Holder  string             `json:"holder"`
	Entries []bulkReleaseEntry `json:"entries"`
}

type waitReq struct {
	Resource   string `json:"resource"`
	Holder     string `json:"holder"`
	TTLSeconds int64  `json:"ttl_seconds"`
}

type holderReq struct {
	ID            string `json:"id"`
	Description   string `json:"description"`
	MaxConcurrent int    `json:"max_concurrent"`
}

type quotaReq struct {
	MaxConcurrent int `json:"max_concurrent"`
}

type policyReq struct {
	Name        string `json:"name"`
	TTLSeconds  int64  `json:"ttl_seconds"`
	Description string `json:"description"`
}

type tagReq struct {
	Tag string `json:"tag"`
}

type leaseResp struct {
	Resource     string `json:"resource"`
	Holder       string `json:"holder"`
	Token        int64  `json:"token"`
	DeadlineUnix int64  `json:"deadline_unix"`
	AcquiredUnix int64  `json:"acquired_unix"`
	TTLSeconds   int64  `json:"ttl_seconds"`
	Active       *bool  `json:"active,omitempty"`
}

type renewResp struct {
	Resource     string `json:"resource"`
	Holder       string `json:"holder"`
	Token        int64  `json:"token"`
	DeadlineUnix int64  `json:"deadline_unix"`
}

type releaseResp struct {
	Resource string `json:"resource"`
	Released bool   `json:"released"`
}

type listResp struct {
	Leases []leaseResp `json:"leases"`
}

type expireResp struct {
	ExpiredCount int `json:"expired_count"`
}

type bulkAcquireResp struct {
	Granted []leaseResp `json:"granted"`
}

type bulkReleaseResp struct {
	Released int `json:"released"`
}

type waiterResp struct {
	ID           string             `json:"id"`
	Resource     string             `json:"resource"`
	Holder       string             `json:"holder"`
	TTLSeconds   int64              `json:"ttl_seconds"`
	Status       lease.WaiterStatus `json:"status"`
	GrantedToken int64              `json:"granted_token,omitempty"`
}

type listWaitersResp struct {
	Waiters []waiterResp `json:"waiters"`
}

type holderResp struct {
	ID            string `json:"id"`
	Description   string `json:"description"`
	MaxConcurrent int    `json:"max_concurrent"`
}

type listHoldersResp struct {
	Holders []holderResp `json:"holders"`
}

type quotaResp struct {
	HolderID      string `json:"holder_id"`
	MaxConcurrent int    `json:"max_concurrent"`
}

type policyResp struct {
	Name        string `json:"name"`
	TTLSeconds  int64  `json:"ttl_seconds"`
	Description string `json:"description"`
}

type listPoliciesResp struct {
	Policies []policyResp `json:"policies"`
}

type listResourcesResp struct {
	Resources []string `json:"resources"`
}

type tagsResp struct {
	Resource string   `json:"resource"`
	Tags     []string `json:"tags"`
}

type listAuditResp struct {
	Entries []lease.AuditEntry `json:"entries"`
}

type errResp struct {
	Error string `json:"error"`
}

// ----------------------------------------------------------------------------
// Helpers
// ----------------------------------------------------------------------------

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, errResp{Error: msg})
}

func decode(r *http.Request, v any) error {
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	return dec.Decode(v)
}

func newLeaseResp(l lease.Lease) leaseResp {
	return leaseResp{
		Resource: l.Resource, Holder: l.Holder, Token: int64(l.Token),
		DeadlineUnix: l.Deadline.Unix(), AcquiredUnix: l.Acquired.Unix(), TTLSeconds: l.TTLSeconds,
	}
}

func newLeaseRespActive(l lease.Lease, active bool) leaseResp {
	r := newLeaseResp(l)
	b := active
	r.Active = &b
	return r
}

func newWaiterResp(w lease.Waiter) waiterResp {
	return waiterResp{
		ID: w.ID, Resource: w.Resource, Holder: w.Holder, TTLSeconds: w.TTLSeconds,
		Status: w.Status, GrantedToken: int64(w.GrantedToken),
	}
}

func newHolderResp(h lease.Holder) holderResp {
	return holderResp{ID: h.ID, Description: h.Description, MaxConcurrent: h.MaxConcurrent}
}

// statusFor maps a lease error to its HTTP status and message.
func statusFor(err error) (int, string) {
	switch {
	case lease.IsInvalid(err):
		return http.StatusBadRequest, err.Error()
	case errors.Is(err, lease.ErrHeld):
		return http.StatusConflict, "resource held"
	case errors.Is(err, lease.ErrNotHeld):
		return http.StatusNotFound, "not held"
	case errors.Is(err, lease.ErrNotHolder):
		return http.StatusConflict, "not holder"
	case errors.Is(err, lease.ErrExpired):
		return http.StatusGone, "expired"
	case errors.Is(err, lease.ErrQuotaExceeded):
		return http.StatusTooManyRequests, "quota exceeded"
	case errors.Is(err, lease.ErrHolderExists):
		return http.StatusConflict, "holder exists"
	case errors.Is(err, lease.ErrHolderNotFound):
		return http.StatusNotFound, "holder not found"
	case errors.Is(err, lease.ErrHolderHasLeases):
		return http.StatusConflict, "holder has active leases"
	case errors.Is(err, lease.ErrHolderHasWaiters):
		return http.StatusConflict, "holder has pending waiters"
	case errors.Is(err, lease.ErrPolicyExists):
		return http.StatusConflict, "policy exists"
	case errors.Is(err, lease.ErrPolicyNotFound):
		return http.StatusNotFound, "policy not found"
	case errors.Is(err, lease.ErrWaiterNotFound):
		return http.StatusNotFound, "waiter not found"
	case errors.Is(err, lease.ErrTagNotFound):
		return http.StatusNotFound, "tag not found"
	case errors.Is(err, lease.ErrConflict):
		return http.StatusConflict, "bulk conflict"
	default:
		return http.StatusBadRequest, err.Error()
	}
}

func writeLeaseErr(w http.ResponseWriter, err error) {
	status, msg := statusFor(err)
	writeErr(w, status, msg)
}

// ----------------------------------------------------------------------------
// Service
// ----------------------------------------------------------------------------

// Service bundles a lease.Service with its store so the caller can close both.
type Service struct {
	svc   *lease.Service
	store *store.Store
}

func (s *Service) close() error { return s.store.Close() }

func newService(dbPath string, clk clock.Clock) (*Service, error) {
	st, err := store.Open(dbPath)
	if err != nil {
		return nil, err
	}
	return &Service{svc: lease.NewService(st, clk), store: st}, nil
}

// ----------------------------------------------------------------------------
// Handlers
// ----------------------------------------------------------------------------

func handleAcquire(svc *lease.Service) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req acquireReq
		if err := decode(r, &req); err != nil {
			writeErr(w, http.StatusBadRequest, "invalid json: "+err.Error())
			return
		}
		l, err := svc.Acquire(req.Resource, req.Holder, req.TTLSeconds, req.Policy)
		if err != nil {
			writeLeaseErr(w, err)
			return
		}
		writeJSON(w, http.StatusOK, newLeaseResp(l))
	}
}

func handleRenew(svc *lease.Service) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req renewReq
		if err := decode(r, &req); err != nil {
			writeErr(w, http.StatusBadRequest, "invalid json: "+err.Error())
			return
		}
		l, err := svc.Renew(req.Resource, req.Holder, lease.Token(req.Token), req.TTLSeconds)
		if err != nil {
			writeLeaseErr(w, err)
			return
		}
		writeJSON(w, http.StatusOK, renewResp{
			Resource: l.Resource, Holder: l.Holder, Token: int64(l.Token), DeadlineUnix: l.Deadline.Unix(),
		})
	}
}

func handleRelease(svc *lease.Service) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req releaseReq
		if err := decode(r, &req); err != nil {
			writeErr(w, http.StatusBadRequest, "invalid json: "+err.Error())
			return
		}
		if err := svc.Release(req.Resource, req.Holder, lease.Token(req.Token)); err != nil {
			writeLeaseErr(w, err)
			return
		}
		writeJSON(w, http.StatusOK, releaseResp{Resource: req.Resource, Released: true})
	}
}

func handleTransfer(svc *lease.Service) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req transferReq
		if err := decode(r, &req); err != nil {
			writeErr(w, http.StatusBadRequest, "invalid json: "+err.Error())
			return
		}
		l, err := svc.Transfer(req.Resource, req.Holder, lease.Token(req.Token), req.NewHolder)
		if err != nil {
			writeLeaseErr(w, err)
			return
		}
		writeJSON(w, http.StatusOK, renewResp{
			Resource: l.Resource, Holder: l.Holder, Token: int64(l.Token), DeadlineUnix: l.Deadline.Unix(),
		})
	}
}

func handleBulkAcquire(svc *lease.Service) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req bulkAcquireReq
		if err := decode(r, &req); err != nil {
			writeErr(w, http.StatusBadRequest, "invalid json: "+err.Error())
			return
		}
		granted, err := svc.BulkAcquire(req.Holder, req.Resources, req.TTLSeconds)
		if err != nil {
			writeLeaseErr(w, err)
			return
		}
		out := make([]leaseResp, 0, len(granted))
		for _, l := range granted {
			out = append(out, newLeaseResp(l))
		}
		writeJSON(w, http.StatusOK, bulkAcquireResp{Granted: out})
	}
}

func handleBulkRelease(svc *lease.Service) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req bulkReleaseReq
		if err := decode(r, &req); err != nil {
			writeErr(w, http.StatusBadRequest, "invalid json: "+err.Error())
			return
		}
		entries := make([]lease.ReleaseEntry, 0, len(req.Entries))
		for _, e := range req.Entries {
			entries = append(entries, lease.ReleaseEntry{Resource: e.Resource, Token: lease.Token(e.Token)})
		}
		n, err := svc.BulkRelease(req.Holder, entries)
		if err != nil {
			writeLeaseErr(w, err)
			return
		}
		writeJSON(w, http.StatusOK, bulkReleaseResp{Released: n})
	}
}

func handleLeasesCollection(svc *lease.Service) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			handleAcquire(svc).ServeHTTP(w, r)
		case http.MethodGet:
			if resource := r.URL.Query().Get("resource"); resource != "" {
				l, active, ok := svc.Inspect(resource)
				if !ok {
					writeErr(w, http.StatusNotFound, "not held")
					return
				}
				writeJSON(w, http.StatusOK, newLeaseRespActive(l, active))
				return
			}
			views, err := svc.List()
			if err != nil {
				writeErr(w, http.StatusInternalServerError, err.Error())
				return
			}
			out := make([]leaseResp, 0, len(views))
			for _, v := range views {
				out = append(out, newLeaseRespActive(v.Lease, v.Active))
			}
			writeJSON(w, http.StatusOK, listResp{Leases: out})
		default:
			writeErr(w, http.StatusMethodNotAllowed, "method not allowed")
		}
	}
}

func handleExpire(svc *lease.Service) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeErr(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		n, err := svc.ExpireAll()
		if err != nil {
			writeErr(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, expireResp{ExpiredCount: n})
	}
}

func handleWait(svc *lease.Service) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeErr(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		var req waitReq
		if err := decode(r, &req); err != nil {
			writeErr(w, http.StatusBadRequest, "invalid json: "+err.Error())
			return
		}
		w0, err := svc.EnqueueWaiter(req.Resource, req.Holder, req.TTLSeconds)
		if err != nil {
			writeLeaseErr(w, err)
			return
		}
		status := http.StatusAccepted
		if w0.Status == lease.WaiterGranted {
			status = http.StatusOK
		}
		writeJSON(w, status, newWaiterResp(w0))
	}
}

func handleWaiterCancel(svc *lease.Service) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			writeErr(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		id := r.PathValue("id")
		if err := svc.CancelWaiter(id); err != nil {
			writeLeaseErr(w, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"id": id, "status": "cancelled"})
	}
}

func handleWaitersList(svc *lease.Service) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeErr(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		resource := r.URL.Query().Get("resource")
		status := lease.WaiterStatus(r.URL.Query().Get("status"))
		waiters, err := svc.ListWaiters(resource, status)
		if err != nil {
			writeErr(w, http.StatusInternalServerError, err.Error())
			return
		}
		out := make([]waiterResp, 0, len(waiters))
		for _, w0 := range waiters {
			out = append(out, newWaiterResp(w0))
		}
		writeJSON(w, http.StatusOK, listWaitersResp{Waiters: out})
	}
}

func handleHolderPut(svc *lease.Service) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req holderReq
		if err := decode(r, &req); err != nil {
			writeErr(w, http.StatusBadRequest, "invalid json: "+err.Error())
			return
		}
		h, err := svc.PutHolder(req.ID, req.Description, req.MaxConcurrent)
		if err != nil {
			writeLeaseErr(w, err)
			return
		}
		writeJSON(w, http.StatusOK, newHolderResp(h))
	}
}

func handleHoldersList(svc *lease.Service) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeErr(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		holders, err := svc.ListHolders()
		if err != nil {
			writeErr(w, http.StatusInternalServerError, err.Error())
			return
		}
		out := make([]holderResp, 0, len(holders))
		for _, h := range holders {
			out = append(out, newHolderResp(h))
		}
		writeJSON(w, http.StatusOK, listHoldersResp{Holders: out})
	}
}

func handleHolderByID(svc *lease.Service) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		switch r.Method {
		case http.MethodGet:
			h, ok, err := svc.GetHolder(id)
			if err != nil {
				writeErr(w, http.StatusInternalServerError, err.Error())
				return
			}
			if !ok {
				writeErr(w, http.StatusNotFound, "holder not found")
				return
			}
			writeJSON(w, http.StatusOK, newHolderResp(h))
		case http.MethodDelete:
			if err := svc.DeleteHolder(id); err != nil {
				writeLeaseErr(w, err)
				return
			}
			writeJSON(w, http.StatusOK, map[string]string{"id": id, "status": "deleted"})
		default:
			writeErr(w, http.StatusMethodNotAllowed, "method not allowed")
		}
	}
}

func handleQuota(svc *lease.Service) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		switch r.Method {
		case http.MethodPut, http.MethodPost:
			var req quotaReq
			if err := decode(r, &req); err != nil {
				writeErr(w, http.StatusBadRequest, "invalid json: "+err.Error())
				return
			}
			if err := svc.SetQuota(id, req.MaxConcurrent); err != nil {
				writeLeaseErr(w, err)
				return
			}
			q, _, _ := svc.GetQuota(id)
			writeJSON(w, http.StatusOK, quotaResp{HolderID: q.HolderID, MaxConcurrent: q.MaxConcurrent})
		case http.MethodGet:
			q, ok, err := svc.GetQuota(id)
			if err != nil {
				writeErr(w, http.StatusInternalServerError, err.Error())
				return
			}
			if !ok {
				writeErr(w, http.StatusNotFound, "holder not found")
				return
			}
			writeJSON(w, http.StatusOK, quotaResp{HolderID: q.HolderID, MaxConcurrent: q.MaxConcurrent})
		default:
			writeErr(w, http.StatusMethodNotAllowed, "method not allowed")
		}
	}
}

func handlePolicyPut(svc *lease.Service) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req policyReq
		if err := decode(r, &req); err != nil {
			writeErr(w, http.StatusBadRequest, "invalid json: "+err.Error())
			return
		}
		p, err := svc.PutPolicy(req.Name, req.TTLSeconds, req.Description)
		if err != nil {
			writeLeaseErr(w, err)
			return
		}
		writeJSON(w, http.StatusOK, policyResp{Name: p.Name, TTLSeconds: p.TTLSeconds, Description: p.Description})
	}
}

func handlePoliciesList(svc *lease.Service) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeErr(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		policies, err := svc.ListPolicies()
		if err != nil {
			writeErr(w, http.StatusInternalServerError, err.Error())
			return
		}
		out := make([]policyResp, 0, len(policies))
		for _, p := range policies {
			out = append(out, policyResp{Name: p.Name, TTLSeconds: p.TTLSeconds, Description: p.Description})
		}
		writeJSON(w, http.StatusOK, listPoliciesResp{Policies: out})
	}
}

func handleTagAdd(svc *lease.Service) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeErr(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		resource := r.PathValue("id")
		var req tagReq
		if err := decode(r, &req); err != nil {
			writeErr(w, http.StatusBadRequest, "invalid json: "+err.Error())
			return
		}
		if err := svc.AddTag(resource, req.Tag); err != nil {
			writeLeaseErr(w, err)
			return
		}
		tags, _ := svc.GetTags(resource)
		writeJSON(w, http.StatusOK, tagsResp{Resource: resource, Tags: tags})
	}
}

func handleTagRemove(svc *lease.Service) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			writeErr(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		resource := r.PathValue("id")
		tag := r.PathValue("tag")
		if err := svc.RemoveTag(resource, tag); err != nil {
			writeLeaseErr(w, err)
			return
		}
		tags, _ := svc.GetTags(resource)
		writeJSON(w, http.StatusOK, tagsResp{Resource: resource, Tags: tags})
	}
}

func handleTagsGet(svc *lease.Service) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeErr(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		resource := r.PathValue("id")
		tags, err := svc.GetTags(resource)
		if err != nil {
			writeLeaseErr(w, err)
			return
		}
		writeJSON(w, http.StatusOK, tagsResp{Resource: resource, Tags: tags})
	}
}

func handleResourcesList(svc *lease.Service) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeErr(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		tag := r.URL.Query().Get("tag")
		resources, err := svc.ListResources(tag)
		if err != nil {
			writeErr(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, listResourcesResp{Resources: resources})
	}
}

func handleAudit(svc *lease.Service) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeErr(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		resource := r.URL.Query().Get("resource")
		limit, err := parseQueryInt(r.URL.Query().Get("limit"))
		if err != nil {
			writeErr(w, http.StatusBadRequest, "invalid limit")
			return
		}
		offset, err := parseQueryInt(r.URL.Query().Get("offset"))
		if err != nil {
			writeErr(w, http.StatusBadRequest, "invalid offset")
			return
		}
		entries, err := svc.ListAudit(resource, limit, offset)
		if err != nil {
			writeErr(w, http.StatusBadRequest, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, listAuditResp{Entries: entries})
	}
}

func parseQueryInt(raw string) (int, error) {
	if raw == "" {
		return 0, nil
	}
	return strconv.Atoi(raw)
}

func handleTokenHistory(svc *lease.Service) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeErr(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		resource := r.URL.Query().Get("resource")
		entries, err := svc.TokenHistory(resource)
		if err != nil {
			writeErr(w, http.StatusBadRequest, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, listAuditResp{Entries: entries})
	}
}

func handleStats(svc *lease.Service) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeErr(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		st, err := svc.Stats()
		if err != nil {
			writeErr(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, st)
	}
}

func handleDiagnostics(svc *lease.Service) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeErr(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		diag, err := svc.Diagnostics()
		if err != nil {
			writeErr(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, diag)
	}
}

func handleTokenHealth(svc *lease.Service) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeErr(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		health, err := svc.CheckTokenHealth(r.URL.Query().Get("resource"))
		if err != nil {
			writeErr(w, http.StatusBadRequest, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, health)
	}
}

// ----------------------------------------------------------------------------
// Routing
// ----------------------------------------------------------------------------

// newMux builds the HTTP handler tree. Go 1.22+ method+path patterns give
// clean per-route method binding and path-value extraction.
func newMux(svc *lease.Service) http.Handler {
	mux := http.NewServeMux()

	// Leases
	mux.HandleFunc("POST /leases", handleAcquire(svc))
	mux.HandleFunc("GET /leases", handleLeasesCollection(svc))
	mux.HandleFunc("POST /leases/renew", handleRenew(svc))
	mux.HandleFunc("POST /leases/release", handleRelease(svc))
	mux.HandleFunc("POST /leases/transfer", handleTransfer(svc))
	mux.HandleFunc("POST /leases/bulk-acquire", handleBulkAcquire(svc))
	mux.HandleFunc("POST /leases/bulk-release", handleBulkRelease(svc))
	mux.HandleFunc("POST /leases/expire", handleExpire(svc))
	mux.HandleFunc("POST /leases/wait", handleWait(svc))
	mux.HandleFunc("GET /leases/waiters", handleWaitersList(svc))
	mux.HandleFunc("DELETE /leases/waiters/{id}", handleWaiterCancel(svc))

	// Holders
	mux.HandleFunc("POST /holders", handleHolderPut(svc))
	mux.HandleFunc("GET /holders", handleHoldersList(svc))
	mux.HandleFunc("GET /holders/{id}", handleHolderByID(svc))
	mux.HandleFunc("DELETE /holders/{id}", handleHolderByID(svc))
	mux.HandleFunc("PUT /holders/{id}/quota", handleQuota(svc))
	mux.HandleFunc("GET /holders/{id}/quota", handleQuota(svc))

	// Resources & tags
	mux.HandleFunc("POST /resources/{id}/tags", handleTagAdd(svc))
	mux.HandleFunc("GET /resources/{id}/tags", handleTagsGet(svc))
	mux.HandleFunc("DELETE /resources/{id}/tags/{tag}", handleTagRemove(svc))
	mux.HandleFunc("GET /resources", handleResourcesList(svc))

	// Policies
	mux.HandleFunc("POST /policies", handlePolicyPut(svc))
	mux.HandleFunc("GET /policies", handlePoliciesList(svc))

	// Audit
	mux.HandleFunc("GET /audit", handleAudit(svc))
	mux.HandleFunc("GET /audit/tokens", handleTokenHistory(svc))
	mux.HandleFunc("GET /audit/tokens/health", handleTokenHealth(svc))

	// Meta
	mux.HandleFunc("GET /stats", handleStats(svc))
	mux.HandleFunc("GET /diagnostics", handleDiagnostics(svc))
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{"ready": true, "db": "open"})
	})
	mux.HandleFunc("GET /versionz", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"version": "1.0.0", "engine": "bbolt"})
	})
	return mux
}

// osExit is indirected so tests can substitute it.
var osExit = os.Exit

func main() {
	smoke := flag.Bool("smoke-test", false, "run self-check and exit")
	addr := flag.String("addr", ":8080", "listen address")
	dbPath := flag.String("db", "./leases.db", "path to the bbolt lease database")
	flag.Parse()

	if *smoke {
		if err := runSmokeTest(); err != nil {
			fmt.Println("smoke-test: FAIL:", err)
			osExit(1)
		}
		fmt.Println("smoke-test: ok")
		osExit(0)
	}

	svc, err := newService(*dbPath, clock.RealClock{})
	if err != nil {
		log.Fatalf("open service: %v", err)
	}
	defer svc.close()
	if n, err := svc.svc.Recover(); err != nil {
		log.Fatalf("recover: %v", err)
	} else if n > 0 {
		log.Printf("recovered: reaped %d expired leases on startup", n)
	}
	srv := &http.Server{Addr: *addr, Handler: newMux(svc.svc)}
	log.Printf("leaselock service listening on %s (db=%s)", *addr, *dbPath)
	log.Fatal(srv.ListenAndServe())
}

// keep strings referenced (used for query parsing in some handlers).
var _ = strings.TrimSpace
