package domain

import "time"

type TenantGroup struct {
	ID        string
	Name      string
	Tenants   []*Tenant
	Policies  PolicySet
	CreatedAt time.Time
}

type Tenant struct {
	ID              string
	Name            string
	TenantGroupID   string
	Resources       []*Resource
	PolicyOverrides *PolicySet
	CreatedAt       time.Time
}

type Resource struct {
	ID           string
	TenantID     string
	CurrentState ResourceState
	GoalState    ResourceState

	CreatedAt    time.Time
	UpdatedAt    time.Time
}
