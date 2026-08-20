package lease

import (
	"fmt"
	"time"

	"task101-leaselock/internal/clock"
)

// Service applies the coordination business rules on top of a Store. It owns
// the clock and is the single type the HTTP layer talks to. All time-dependent
// decisions read the injected clock so tests are deterministic.
type Service struct {
	store Store
	clk   clock.Clock
}

// NewManager is an alias retained for compatibility; NewService is preferred.
func NewManager(store Store, clk clock.Clock) *Service { return NewService(store, clk) }

// NewService binds a store to a clock.
func NewService(store Store, clk clock.Clock) *Service {
	return &Service{store: store, clk: clk}
}

// Clock returns the clock the service reads.
func (s *Service) Clock() clock.Clock { return s.clk }

// Store returns the underlying store (used by main for Close).
func (s *Service) Store() Store { return s.store }

// Recover reaps leases that expired while the process was down. Call once at
// startup. Returns the count reaped.
func (s *Service) Recover() (int, error) { return s.store.ExpireAll(s.clk.Now()) }

// ttlFor resolves the effective TTL for an acquire given either an explicit
// ttlSeconds or a policy name. Exactly one must be supplied.
func (s *Service) ttlFor(ttlSeconds int64, policy string) (time.Duration, error) {
	if policy != "" {
		if ttlSeconds != 0 {
			return 0, fmt.Errorf("%w: provide either ttl_seconds or policy, not both", ErrInvalid)
		}
		p, ok, err := s.store.GetPolicy(policy)
		if err != nil {
			return 0, err
		}
		if !ok {
			return 0, fmt.Errorf("%w: policy %q not found", ErrPolicyNotFound, policy)
		}
		return time.Duration(p.TTLSeconds) * time.Second, nil
	}
	if err := validateTTL(ttlSeconds); err != nil {
		return 0, err
	}
	return time.Duration(ttlSeconds) * time.Second, nil
}

// validateTTL checks that ttl is within [MinTTLSeconds, MaxTTLSeconds].
func validateTTL(ttl int64) error {
	if ttl < MinTTLSeconds || ttl > MaxTTLSeconds {
		return fmt.Errorf("%w: ttl_seconds must be in [%d, %d]", ErrInvalid, MinTTLSeconds, MaxTTLSeconds)
	}
	return nil
}

// Acquire grants a new lease. An expired lease is reaped and replaced with a
// fresh one carrying a strictly larger token.
func (s *Service) Acquire(resource, holder string, ttlSeconds int64, policy string) (Lease, error) {
	if err := validateResourceHolder(resource, holder); err != nil {
		return Lease{}, err
	}
	ttl, err := s.ttlFor(ttlSeconds, policy)
	if err != nil {
		return Lease{}, err
	}
	return s.store.Acquire(resource, holder, s.clk.Now(), ttl)
}

// Renew refreshes the caller's lease deadline to now+ttl, keeping the token.
func (s *Service) Renew(resource, holder string, token Token, ttlSeconds int64) (Lease, error) {
	if err := validateResourceHolder(resource, holder); err != nil {
		return Lease{}, err
	}
	if err := validateTTL(ttlSeconds); err != nil {
		return Lease{}, err
	}
	return s.store.Renew(resource, holder, token, s.clk.Now(), time.Duration(ttlSeconds)*time.Second)
}

// Release drops the caller's active lease; an expired lease is not releasable.
func (s *Service) Release(resource, holder string, token Token) error {
	if err := validateResourceHolder(resource, holder); err != nil {
		return err
	}
	return s.store.Release(resource, holder, token, s.clk.Now())
}

// Transfer hands the lease to newHolder, keeping the token and deadline.
func (s *Service) Transfer(resource, holder string, token Token, newHolder string) (Lease, error) {
	if err := validateResourceHolder(resource, holder); err != nil {
		return Lease{}, err
	}
	if newHolder == "" {
		return Lease{}, fmt.Errorf("%w: new_holder must not be empty", ErrInvalid)
	}
	if newHolder == holder {
		return Lease{}, fmt.Errorf("%w: new_holder must differ from holder", ErrInvalid)
	}
	return s.store.Transfer(resource, holder, token, newHolder, s.clk.Now())
}

// BulkAcquire acquires leases on every resource atomically (all-or-nothing).
func (s *Service) BulkAcquire(holder string, resources []string, ttlSeconds int64) ([]Lease, error) {
	if holder == "" {
		return nil, fmt.Errorf("%w: holder must not be empty", ErrInvalid)
	}
	if len(resources) == 0 {
		return nil, fmt.Errorf("%w: resources must not be empty", ErrInvalid)
	}
	if len(resources) > MaxBulkSize {
		return nil, fmt.Errorf("%w: bulk size exceeds %d", ErrInvalid, MaxBulkSize)
	}
	seen := map[string]bool{}
	for _, r := range resources {
		if r == "" {
			return nil, fmt.Errorf("%w: resource must not be empty", ErrInvalid)
		}
		if seen[r] {
			return nil, fmt.Errorf("%w: duplicate resource %q in bulk", ErrInvalid, r)
		}
		seen[r] = true
	}
	if err := validateTTL(ttlSeconds); err != nil {
		return nil, err
	}
	return s.store.BulkAcquire(holder, resources, s.clk.Now(), time.Duration(ttlSeconds)*time.Second)
}

// BulkRelease releases multiple leases atomically (all-or-nothing).
func (s *Service) BulkRelease(holder string, entries []ReleaseEntry) (int, error) {
	if holder == "" {
		return 0, fmt.Errorf("%w: holder must not be empty", ErrInvalid)
	}
	if len(entries) == 0 {
		return 0, fmt.Errorf("%w: entries must not be empty", ErrInvalid)
	}
	if len(entries) > MaxBulkSize {
		return 0, fmt.Errorf("%w: bulk size exceeds %d", ErrInvalid, MaxBulkSize)
	}
	seen := make(map[string]bool, len(entries))
	for _, entry := range entries {
		if entry.Resource == "" || seen[entry.Resource] {
			return 0, fmt.Errorf("%w: duplicate or empty resource in bulk release", ErrInvalid)
		}
		seen[entry.Resource] = true
	}
	return s.store.BulkRelease(holder, entries, s.clk.Now())
}

// Inspect returns the lease for resource and its liveness.
func (s *Service) Inspect(resource string) (Lease, bool, bool) {
	return s.store.Inspect(resource, s.clk.Now())
}

// List returns every lease with its active flag, sorted by resource.
func (s *Service) List() ([]LeaseView, error) { return s.store.List(s.clk.Now()) }

// ExpireAll reaps dead leases and returns the count.
func (s *Service) ExpireAll() (int, error) { return s.store.ExpireAll(s.clk.Now()) }

// EnqueueWaiter queues a waiter (or grants immediately if the resource is free).
func (s *Service) EnqueueWaiter(resource, holder string, ttlSeconds int64) (Waiter, error) {
	if err := validateResourceHolder(resource, holder); err != nil {
		return Waiter{}, err
	}
	if err := validateTTL(ttlSeconds); err != nil {
		return Waiter{}, err
	}
	return s.store.EnqueueWaiter(resource, holder, time.Duration(ttlSeconds)*time.Second, s.clk.Now())
}

// CancelWaiter cancels a pending waiter.
func (s *Service) CancelWaiter(id string) error { return s.store.CancelWaiter(id) }

// ListWaiters lists waiters, optionally filtered by resource and status.
func (s *Service) ListWaiters(resource string, status WaiterStatus) ([]Waiter, error) {
	return s.store.ListWaiters(resource, status)
}

// PutHolder registers or updates a holder.
func (s *Service) PutHolder(id, description string, maxConcurrent int) (Holder, error) {
	if id == "" {
		return Holder{}, fmt.Errorf("%w: holder id must not be empty", ErrInvalid)
	}
	if maxConcurrent < 0 {
		return Holder{}, fmt.Errorf("%w: max_concurrent must be >= 0", ErrInvalid)
	}
	h := Holder{ID: id, Description: description, MaxConcurrent: maxConcurrent, CreatedAt: s.clk.Now()}
	if err := s.store.PutHolder(h); err != nil {
		return Holder{}, err
	}
	return h, nil
}

// GetHolder returns a holder.
func (s *Service) GetHolder(id string) (Holder, bool, error) { return s.store.GetHolder(id) }

// DeleteHolder removes a holder that holds no active leases.
func (s *Service) DeleteHolder(id string) error {
	if id == "" {
		return fmt.Errorf("%w: holder id must not be empty", ErrInvalid)
	}
	return s.store.DeleteHolder(id, s.clk.Now())
}

// ListHolders lists all registered holders.
func (s *Service) ListHolders() ([]Holder, error) { return s.store.ListHolders() }

// SetQuota sets a holder's concurrency cap.
func (s *Service) SetQuota(id string, max int) error {
	if id == "" {
		return fmt.Errorf("%w: holder id must not be empty", ErrInvalid)
	}
	if max < 0 {
		return fmt.Errorf("%w: max_concurrent must be >= 0", ErrInvalid)
	}
	return s.store.SetQuota(id, max)
}

// GetQuota returns a holder's quota.
func (s *Service) GetQuota(id string) (Quota, bool, error) { return s.store.GetQuota(id) }

// PutPolicy creates a TTL policy. Fails if the name exists.
func (s *Service) PutPolicy(name string, ttlSeconds int64, description string) (Policy, error) {
	if name == "" {
		return Policy{}, fmt.Errorf("%w: policy name must not be empty", ErrInvalid)
	}
	if err := validateTTL(ttlSeconds); err != nil {
		return Policy{}, err
	}
	p := Policy{Name: name, TTLSeconds: ttlSeconds, Description: description}
	if err := s.store.PutPolicy(p); err != nil {
		return Policy{}, err
	}
	return p, nil
}

// ListPolicies lists all policies.
func (s *Service) ListPolicies() ([]Policy, error) { return s.store.ListPolicies() }

// AddTag tags a resource (idempotent).
func (s *Service) AddTag(resource, tag string) error {
	if err := validateTagArgs(resource, tag); err != nil {
		return err
	}
	return s.store.AddTag(resource, tag)
}

// RemoveTag removes a tag from a resource.
func (s *Service) RemoveTag(resource, tag string) error {
	if err := validateTagArgs(resource, tag); err != nil {
		return err
	}
	return s.store.RemoveTag(resource, tag)
}

// ListResources lists resources carrying tag (or all tagged resources if tag
// is empty), sorted ascending.
func (s *Service) ListResources(tag string) ([]string, error) { return s.store.ListResources(tag) }

// GetTags returns the tags on a resource.
func (s *Service) GetTags(resource string) ([]string, error) {
	if resource == "" {
		return nil, fmt.Errorf("%w: resource must not be empty", ErrInvalid)
	}
	return s.store.GetTags(resource)
}

// ListAudit returns audit entries, optionally filtered by resource, with limit
// and offset (ascending by seq).
func (s *Service) ListAudit(resource string, limit, offset int) ([]AuditEntry, error) {
	if limit < 0 || offset < 0 {
		return nil, fmt.Errorf("%w: limit/offset must be >= 0", ErrInvalid)
	}
	if limit == 0 {
		limit = 100
	}
	if limit > 1000 {
		limit = 1000
	}
	return s.store.ListAudit(resource, limit, offset)
}

// TokenHistory returns the token-issuance events for a resource.
func (s *Service) TokenHistory(resource string) ([]AuditEntry, error) {
	if resource == "" {
		return nil, fmt.Errorf("%w: resource must not be empty", ErrInvalid)
	}
	return s.store.TokenHistory(resource)
}

// Stats returns aggregate counters.
func (s *Service) Stats() (Stats, error) { return s.store.Stats(s.clk.Now()) }

// NextToken reports the fencing token the next acquire would receive.
func (s *Service) NextToken() (Token, error) { return s.store.NextToken() }

// Close releases the store.
func (s *Service) Close() error { return s.store.Close() }

func validateResourceHolder(resource, holder string) error {
	if resource == "" {
		return fmt.Errorf("%w: resource must not be empty", ErrInvalid)
	}
	if holder == "" {
		return fmt.Errorf("%w: holder must not be empty", ErrInvalid)
	}
	return nil
}

func validateTagArgs(resource, tag string) error {
	if resource == "" {
		return fmt.Errorf("%w: resource must not be empty", ErrInvalid)
	}
	if tag == "" {
		return fmt.Errorf("%w: tag must not be empty", ErrInvalid)
	}
	return nil
}
