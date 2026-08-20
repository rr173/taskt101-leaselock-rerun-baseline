package lease

import "time"

// LeaseStore covers the core lease lifecycle. Each mutating method runs in one
// transaction: the fencing-token allocation, the lease write, any waiter
// promotion, and the audit append all commit together, so a crash mid-op
// leaves neither partial state nor a dangling audit record.
type LeaseStore interface {
	Acquire(resource, holder string, now time.Time, ttl time.Duration) (Lease, error)
	Renew(resource, holder string, token Token, now time.Time, ttl time.Duration) (Lease, error)
	Release(resource, holder string, token Token, now time.Time) error
	Transfer(resource, holder string, token Token, newHolder string, now time.Time) (Lease, error)
	BulkAcquire(holder string, resources []string, now time.Time, ttl time.Duration) ([]Lease, error)
	BulkRelease(holder string, entries []ReleaseEntry, now time.Time) (int, error)
	Inspect(resource string, now time.Time) (Lease, bool, bool)
	List(now time.Time) ([]LeaseView, error)
	ExpireAll(now time.Time) (int, error)
}

// ReleaseEntry identifies one lease to release in a bulk operation.
type ReleaseEntry struct {
	Resource string
	Token    Token
}

// HolderStore covers holder registration and per-holder concurrency quota.
type HolderStore interface {
	PutHolder(h Holder) error
	GetHolder(id string) (Holder, bool, error)
	DeleteHolder(id string, now time.Time) error
	ListHolders() ([]Holder, error)
	SetQuota(id string, max int) error
	GetQuota(id string) (Quota, bool, error)
}

// PolicyStore covers TTL policy presets.
type PolicyStore interface {
	PutPolicy(p Policy) error
	GetPolicy(name string) (Policy, bool, error)
	ListPolicies() ([]Policy, error)
}

// WaiterStore covers the waiter queue. EnqueueWaiter grants a lease
// immediately if the resource is free and returns a waiter in the granted
// status; otherwise it queues a pending waiter.
type WaiterStore interface {
	EnqueueWaiter(resource, holder string, ttl time.Duration, now time.Time) (Waiter, error)
	CancelWaiter(id string) error
	ListWaiters(resource string, status WaiterStatus) ([]Waiter, error)
}

// TagStore covers independent resource tags.
type TagStore interface {
	AddTag(resource, tag string) error
	RemoveTag(resource, tag string) error
	ListResources(tag string) ([]string, error)
	GetTags(resource string) ([]string, error)
}

// AuditStore covers read access to the append-only audit log. Entries are
// appended internally by the mutating lease operations.
type AuditStore interface {
	ListAudit(resource string, limit, offset int) ([]AuditEntry, error)
	TokenHistory(resource string) ([]AuditEntry, error)
}

// StatsStore returns an aggregate snapshot.
type StatsStore interface {
	Stats(now time.Time) (Stats, error)
}

// DiagnosticsStore exposes consistent operational snapshots for monitoring.
type DiagnosticsStore interface {
	HolderUsage(now time.Time) ([]HolderUsage, error)
	WaiterSummary() (WaiterSummary, error)
	ResourceStates(now time.Time) ([]ResourceState, error)
}

// MetaStore exposes the fencing counter and lifecycle.
type MetaStore interface {
	NextToken() (Token, error)
	Close() error
}

// Store is the full persistence contract the Service depends on. A concrete
// implementation (package store) satisfies every sub-interface.
type Store interface {
	LeaseStore
	HolderStore
	PolicyStore
	WaiterStore
	TagStore
	AuditStore
	StatsStore
	DiagnosticsStore
	MetaStore
}
