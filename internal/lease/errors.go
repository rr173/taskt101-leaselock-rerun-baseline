package lease

import "errors"

// Domain errors. Each maps to a distinct HTTP status via the HTTP layer; the
// store returns these sentinels (wrapped where a reason is useful) and the
// service passes them through.
var (
	// ErrInvalid: malformed or out-of-range field. HTTP 400.
	ErrInvalid = errors.New("invalid request")

	// ErrHeld: the resource already has an active lease. HTTP 409.
	ErrHeld = errors.New("resource held")

	// ErrNotHeld: the resource has no lease record. HTTP 404.
	ErrNotHeld = errors.New("not held")

	// ErrNotHolder: the caller's holder or token does not match the current
	// lease (the caller is a stale holder). HTTP 409.
	ErrNotHolder = errors.New("not holder")

	// ErrExpired: the lease exists and belongs to the caller but its deadline
	// has passed; renew/release are forbidden. HTTP 410.
	ErrExpired = errors.New("expired")

	// ErrQuotaExceeded: the holder has reached its MaxConcurrent cap. HTTP 429.
	ErrQuotaExceeded = errors.New("quota exceeded")

	// ErrHolderExists: a holder with the given id is already registered.
	// HTTP 409.
	ErrHolderExists = errors.New("holder exists")

	// ErrHolderNotFound: no holder registered with that id. HTTP 404.
	ErrHolderNotFound = errors.New("holder not found")

	// ErrHolderHasLeases: the holder still holds active leases and cannot be
	// deleted. HTTP 409.
	ErrHolderHasLeases = errors.New("holder has active leases")

	// ErrHolderHasWaiters: pending waiters still reference the holder, so the
	// holder cannot be removed until those requests are cancelled or granted.
	ErrHolderHasWaiters = errors.New("holder has pending waiters")

	// ErrPolicyExists: a policy with the given name already exists. HTTP 409.
	ErrPolicyExists = errors.New("policy exists")

	// ErrPolicyNotFound: no policy with that name. HTTP 404.
	ErrPolicyNotFound = errors.New("policy not found")

	// ErrWaiterNotFound: no waiter with that id, or it is no longer pending.
	// HTTP 404.
	ErrWaiterNotFound = errors.New("waiter not found")

	// ErrTagNotFound: the resource does not carry the given tag. HTTP 404.
	ErrTagNotFound = errors.New("tag not found")

	// ErrConflict: a bulk operation could not complete atomically because one
	// of its members was in an incompatible state. HTTP 409.
	ErrConflict = errors.New("bulk conflict")
)

// IsInvalid reports whether err wraps ErrInvalid.
func IsInvalid(err error) bool { return errors.Is(err, ErrInvalid) }
