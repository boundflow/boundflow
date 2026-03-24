package storage

import (
	"context"

	"github.com/convergeplane/convergeplane/internal/domain"
)

type TenantGroupRepository interface {
	Create(ctx context.Context, group *domain.TenantGroup) error
	Get(ctx context.Context, id string) (*domain.TenantGroup, error)
}

type TenantRepository interface {
	Create(ctx context.Context, tenant *domain.Tenant) error
	Get(ctx context.Context, tenantGroupID, id string) (*domain.Tenant, error)
}

type ResourceRepository interface {
	Create(ctx context.Context, resource *domain.Resource) error
	Get(ctx context.Context, tenantID, id string) (*domain.Resource, error)
	Update(ctx context.Context, resource *domain.Resource) error
}
