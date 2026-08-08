package service

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"

	"github.com/boundflow/boundflow/internal/domain"
	"github.com/boundflow/boundflow/internal/pricing"
	"github.com/boundflow/boundflow/internal/storage"
)

const DefaultTenantGroupID = "default"

type RegistrationService struct {
	tenantGroups  storage.TenantGroupRepository
	tenants       storage.TenantRepository
	modelPricing  storage.ModelPricingRepository
	numPartitions int
	log           *slog.Logger
}

func NewRegistrationService(
	tenantGroups storage.TenantGroupRepository,
	tenants storage.TenantRepository,
	modelPricing storage.ModelPricingRepository,
	numPartitions int,
	log *slog.Logger,
) *RegistrationService {
	return &RegistrationService{
		tenantGroups:  tenantGroups,
		tenants:       tenants,
		modelPricing:  modelPricing,
		numPartitions: numPartitions,
		log:           log.With("component", "registration_service"),
	}
}

// SetModelPricing overrides a model's rate for the tenant group.
func (s *RegistrationService) SetModelPricing(ctx context.Context, tenantGroupID string, p domain.ModelPricing) error {
	if err := s.modelPricing.Upsert(ctx, tenantGroupID, p); err != nil {
		return fmt.Errorf("set model pricing: %w", err)
	}
	return nil
}

// ListEffectivePricing returns the tenant group's effective rates — the built-in
// defaults merged with its overrides.
func (s *RegistrationService) ListEffectivePricing(ctx context.Context, tenantGroupID string) ([]domain.ModelPricing, error) {
	defaults, err := s.modelPricing.ListDefaults(ctx)
	if err != nil {
		return nil, fmt.Errorf("list default pricing: %w", err)
	}
	overrides, err := s.modelPricing.ListForTenantGroup(ctx, tenantGroupID)
	if err != nil {
		return nil, fmt.Errorf("list model pricing: %w", err)
	}
	effective := pricing.Effective(defaults, overrides)
	out := make([]domain.ModelPricing, 0, len(effective))
	for _, p := range effective {
		out = append(out, p)
	}
	return out, nil
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
	tenant.SchedulerPartitionID = partitionForID(tenant.ID, s.numPartitions)

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

func (s *RegistrationService) ListTenants(ctx context.Context, tenantGroupID string) ([]*domain.Tenant, error) {
	tenants, err := s.tenants.ListForTenantGroup(ctx, tenantGroupID)
	if err != nil {
		return nil, fmt.Errorf("list tenants: %w", err)
	}
	return tenants, nil
}

func (s *RegistrationService) DeleteTenant(ctx context.Context, id string) error {
	if err := s.tenants.MarkDeleted(ctx, id); err != nil {
		return fmt.Errorf("delete tenant: %w", err)
	}

	if _, err := s.tenants.PurgeIfEmpty(ctx, id); err != nil {
		s.log.Error("best-effort tenant purge failed, periodic reconciler will retry", "tenant_id", id, "error", err)
	}

	return nil
}
