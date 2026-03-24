package service

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/convergeplane/convergeplane/internal/domain"
	"github.com/convergeplane/convergeplane/internal/storage"
)

const DefaultTenantGroupID = "default"

type RegistrationService struct {
	tenantGroups storage.TenantGroupRepository
	tenants      storage.TenantRepository
}

func NewRegistrationService(
	tenantGroups storage.TenantGroupRepository,
	tenants storage.TenantRepository,
) *RegistrationService {
	return &RegistrationService{
		tenantGroups: tenantGroups,
		tenants:      tenants,
	}
}

func (s *RegistrationService) CreateTenantGroup(ctx context.Context, group *domain.TenantGroup) (*domain.TenantGroup, error) {
	group.ID = uuid.New().String()
	group.CreatedAt = time.Now()

	if err := s.tenantGroups.Create(ctx, group); err != nil {
		return nil, fmt.Errorf("create tenant group: %w", err)
	}

	return group, nil
}

func (s *RegistrationService) GetTenantGroup(ctx context.Context, id string) (*domain.TenantGroup, error) {
	group, err := s.tenantGroups.Get(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("get tenant group: %w", err)
	}
	return group, nil
}

func (s *RegistrationService) DeleteTenantGroup(ctx context.Context, id string) error {
	if err := s.tenantGroups.Delete(ctx, id); err != nil {
		return fmt.Errorf("delete tenant group: %w", err)
	}
	return nil
}

func (s *RegistrationService) CreateTenant(ctx context.Context, tenant *domain.Tenant) (*domain.Tenant, error) {
	tenant.ID = uuid.New().String()
	tenant.CreatedAt = time.Now()

	if tenant.TenantGroupID == "" {
		tenant.TenantGroupID = DefaultTenantGroupID
	}

	if err := s.tenants.Create(ctx, tenant); err != nil {
		return nil, fmt.Errorf("create tenant: %w", err)
	}

	return tenant, nil
}

func (s *RegistrationService) GetTenant(ctx context.Context, id string) (*domain.Tenant, error) {
	tenant, err := s.tenants.Get(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("get tenant: %w", err)
	}
	return tenant, nil
}

func (s *RegistrationService) DeleteTenant(ctx context.Context, id string) error {
	if err := s.tenants.Delete(ctx, id); err != nil {
		return fmt.Errorf("delete tenant: %w", err)
	}
	return nil
}
