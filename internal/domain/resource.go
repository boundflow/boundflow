package domain

import "time"

type Resource struct {
	ID           string
	TenantID     string
	ResourceType string
	CurrentState ResourceState
	GoalState    ResourceState
	CreatedAt    time.Time
	UpdatedAt    time.Time
}
