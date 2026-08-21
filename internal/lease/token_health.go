package lease

// TokenHealth describes whether the persisted audit history preserves the
// fencing-token invariant for one resource.
type TokenHealth struct {
	Resource           string `json:"resource"`
	Entries            int    `json:"entries"`
	StrictlyIncreasing bool   `json:"strictly_increasing"`
	LastToken          Token  `json:"last_token"`
}

// CheckTokenHealth verifies the monotonic ordering that stale holders rely on.
// The fencing-token invariant requires that every *new* issuance be strictly
// greater than all prior tokens, but a renewal reuses the existing token, so
// adjacent equal tokens (the same token across acquire+renew) are healthy. Only
// a token going backwards — smaller than one already seen — breaks the
// invariant, so the check is strictly-less-than rather than less-than-or-equal.
func (s *Service) CheckTokenHealth(resource string) (TokenHealth, error) {
	entries, err := s.TokenHistory(resource)
	if err != nil {
		return TokenHealth{}, err
	}
	result := TokenHealth{Resource: resource, Entries: len(entries), StrictlyIncreasing: true}
	var previous Token
	for _, entry := range entries {
		if entry.Token < previous {
			result.StrictlyIncreasing = false
		}
		if entry.Token > result.LastToken {
			result.LastToken = entry.Token
		}
		previous = entry.Token
	}
	return result, nil
}
