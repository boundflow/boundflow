package domain

import "time"

// ResourceInstance is an actual instance of a resource tied to a tenant,
// managed via the ResourceLifecycleService.
type ResourceInstance struct {
	ID           string
	ResourceType string
	TenantID     string
	CurrentState ResourceState
	GoalState    ResourceState
	CreatedAt    time.Time
	UpdatedAt    time.Time
}
