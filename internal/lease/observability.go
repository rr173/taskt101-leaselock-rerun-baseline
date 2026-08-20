package lease

import "time"

// HolderUsage is the operational view used to explain quota pressure.
type HolderUsage struct {
	HolderID      string `json:"holder_id"`
	ActiveLeases  int    `json:"active_leases"`
	QueuedWaiters int    `json:"queued_waiters"`
}

// WaiterSummary groups the persisted queue by lifecycle state.
type WaiterSummary struct {
	Total     int `json:"total"`
	Pending   int `json:"pending"`
	Granted   int `json:"granted"`
	Cancelled int `json:"cancelled"`
}

// ResourceState is a point-in-time view of every persisted resource lease.
type ResourceState struct {
	Resource string    `json:"resource"`
	Holder   string    `json:"holder"`
	Token    Token     `json:"token"`
	Active   bool      `json:"active"`
	Deadline time.Time `json:"-"`
}

// Diagnostics gathers the operational views in one consistent request.
type Diagnostics struct {
	Holders   []HolderUsage   `json:"holders"`
	Waiters   WaiterSummary   `json:"waiters"`
	Resources []ResourceState `json:"resources"`
}

// HolderUsage returns active leases and queued waiters grouped by holder.
func (s *Service) HolderUsage() ([]HolderUsage, error) { return s.store.HolderUsage(s.clk.Now()) }

// WaiterSummary returns persisted queue counts by lifecycle state.
func (s *Service) WaiterSummary() (WaiterSummary, error) { return s.store.WaiterSummary() }

// ResourceStates returns a consistent snapshot of lease liveness.
func (s *Service) ResourceStates() ([]ResourceState, error) {
	return s.store.ResourceStates(s.clk.Now())
}

// Diagnostics returns the three operational views used by the diagnostics API.
func (s *Service) Diagnostics() (Diagnostics, error) {
	holders, err := s.HolderUsage()
	if err != nil {
		return Diagnostics{}, err
	}
	waiters, err := s.WaiterSummary()
	if err != nil {
		return Diagnostics{}, err
	}
	resources, err := s.ResourceStates()
	if err != nil {
		return Diagnostics{}, err
	}
	return Diagnostics{Holders: holders, Waiters: waiters, Resources: resources}, nil
}
