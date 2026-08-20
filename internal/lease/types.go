// Package lease defines the domain of a distributed coordination service
// built around exclusive, time-bounded leases with globally monotonic fencing
// tokens. The package owns the domain types, the error vocabulary, the
// persistence contract (the Store interface), and the Service that applies the
// business rules. The bbolt-backed implementation lives in package store.
//
// Subsystems:
//   - Leases: acquire/renew/release/transfer/inspect/list/expire/bulk.
//   - Holders & quota: register holders, enforce per-holder concurrency caps.
//   - Policies: named TTL presets referenced by acquire.
//   - Waiters: FIFO queue of clients waiting for a held resource; promoted
//     automatically when a lease is released or expires.
//   - Tags: independent resource metadata with tag-filtered listing.
//   - Audit: append-only history of every state change.
//   - Stats: aggregate counters.
package lease

import "time"

// Token is a fencing token: a positive integer that increases strictly with
// every new lease granted anywhere in the system, persisted so the ordering
// survives restarts. A stale holder presenting an old token is rejected when
// the resource has since been re-acquired under a larger token.
type Token int64

// Lease is an exclusive, time-bounded hold on a resource. The zero value is
// not a valid lease.
type Lease struct {
	Resource   string    `json:"resource"`
	Holder     string    `json:"holder"`
	Token      Token     `json:"token"`
	Deadline   time.Time `json:"-"`
	Acquired   time.Time `json:"-"`
	TTLSeconds int64     `json:"ttl_seconds"`
}

// Active reports whether the lease is still alive at the given instant. At the
// exact deadline the lease is already considered dead.
func (l Lease) Active(now time.Time) bool { return l.Deadline.After(now) }

// LeaseView pairs a lease with a precomputed active flag, as returned by List.
type LeaseView struct {
	Lease  Lease
	Active bool
}

// Holder is a registered client that may hold leases. MaxConcurrent of 0 means
// unlimited; a positive value caps the number of active leases the holder may
// hold at once.
type Holder struct {
	ID            string    `json:"id"`
	Description   string    `json:"description"`
	MaxConcurrent int       `json:"max_concurrent"`
	CreatedAt     time.Time `json:"-"`
}

// Quota is the effective concurrency limit for a holder.
type Quota struct {
	HolderID      string `json:"holder_id"`
	MaxConcurrent int    `json:"max_concurrent"`
}

// Policy is a named TTL preset that acquire can reference by name instead of
// specifying ttl_seconds directly.
type Policy struct {
	Name        string `json:"name"`
	TTLSeconds  int64  `json:"ttl_seconds"`
	Description string `json:"description"`
}

// WaiterStatus is the lifecycle state of a waiter.
type WaiterStatus string

const (
	WaiterPending   WaiterStatus = "pending"
	WaiterGranted   WaiterStatus = "granted"
	WaiterCancelled WaiterStatus = "cancelled"
)

// Waiter is a client queued for a currently-held resource. When the resource
// is freed (release or expiry), the oldest pending waiter for that resource is
// promoted: a fresh lease is granted to it and its status becomes granted.
type Waiter struct {
	ID         string      `json:"id"`
	Resource   string      `json:"resource"`
	Holder     string      `json:"holder"`
	TTLSeconds int64       `json:"ttl_seconds"`
	CreatedAt  time.Time   `json:"-"`
	Status     WaiterStatus `json:"status"`
	GrantedToken Token     `json:"granted_token,omitempty"`
}

// AuditAction is the kind of event recorded in the audit log.
type AuditAction string

const (
	ActionAcquire      AuditAction = "acquire"
	ActionRenew        AuditAction = "renew"
	ActionRelease      AuditAction = "release"
	ActionTransfer     AuditAction = "transfer"
	ActionExpire       AuditAction = "expire"
	ActionBulkAcquire  AuditAction = "bulk_acquire"
	ActionBulkRelease  AuditAction = "bulk_release"
	ActionWaiterGrant  AuditAction = "waiter_grant"
	ActionQuotaExceed  AuditAction = "quota_exceeded"
)

// AuditEntry is one record in the append-only audit log.
type AuditEntry struct {
	Seq      int64       `json:"seq"`
	Time     time.Time   `json:"-"`
	Action   AuditAction `json:"action"`
	Resource string      `json:"resource,omitempty"`
	Holder   string      `json:"holder,omitempty"`
	Token    Token       `json:"token,omitempty"`
	Detail   string      `json:"detail,omitempty"`
}

// Stats is the aggregate snapshot returned by the stats endpoint.
type Stats struct {
	TotalLeases     int   `json:"total_leases"`
	ActiveLeases    int   `json:"active_leases"`
	ExpiredLeases   int   `json:"expired_leases"`
	NextFencingToken Token `json:"next_fencing_token"`
	RegisteredHolders int  `json:"registered_holders"`
	TotalWaiters    int   `json:"total_waiters"`
	PendingWaiters  int   `json:"pending_waiters"`
	PoliciesCount   int   `json:"policies_count"`
	AuditEntries    int64 `json:"audit_entries"`
}

// Limits on requested lifetimes.
const (
	MinTTLSeconds int64 = 1
	MaxTTLSeconds int64 = 3600
)

// MaxBulkSize bounds the number of resources in a single bulk operation.
const MaxBulkSize = 256
