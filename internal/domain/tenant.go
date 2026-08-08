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
	ID                   string
	Name                 string
	TenantGroupID        string
	PolicyOverrides      *PolicySet
	CreatedAt            time.Time
	DeletedAt            *time.Time
	SchedulerPartitionID string
}
