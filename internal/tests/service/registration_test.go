package service_test

import (
	"context"
	"testing"

	"go.uber.org/mock/gomock"

	"github.com/convergeplane/convergeplane/internal/domain"
	"github.com/convergeplane/convergeplane/internal/service"
	"github.com/convergeplane/convergeplane/internal/storage/mocks"
)

func TestCreateTenantGroup(t *testing.T) {
	ctrl := gomock.NewController(t)
	tenantGroupRepo := mocks.NewMockTenantGroupRepository(ctrl)
	tenantRepo := mocks.NewMockTenantRepository(ctrl)
	modelPricingRepo := mocks.NewMockModelPricingRepository(ctrl)
	svc := service.NewRegistrationService(tenantGroupRepo, tenantRepo, modelPricingRepo)

	tenantGroupRepo.EXPECT().
		Create(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, g *domain.TenantGroup) error {
			if g.ID == "" {
				t.Error("expected ID to be generated")
			}
			if g.CreatedAt.IsZero() {
				t.Error("expected created_at to be set")
			}
			return nil
		})

	group, err := svc.CreateTenantGroup(context.Background(), &domain.TenantGroup{
		Name: "production",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if group.ID == "" {
		t.Error("expected group to have an ID")
	}
	if group.Name != "production" {
		t.Errorf("expected name production, got %s", group.Name)
	}
}

func TestCreateTenant(t *testing.T) {
	ctrl := gomock.NewController(t)
	tenantGroupRepo := mocks.NewMockTenantGroupRepository(ctrl)
	tenantRepo := mocks.NewMockTenantRepository(ctrl)
	modelPricingRepo := mocks.NewMockModelPricingRepository(ctrl)
	svc := service.NewRegistrationService(tenantGroupRepo, tenantRepo, modelPricingRepo)

	t.Run("with explicit tenant_group_id", func(t *testing.T) {
		tenantRepo.EXPECT().
			Create(gomock.Any(), gomock.Any()).
			DoAndReturn(func(_ context.Context, tenant *domain.Tenant) error {
				if tenant.TenantGroupID != "group-1" {
					t.Errorf("expected tenant_group_id group-1, got %s", tenant.TenantGroupID)
				}
				return nil
			})

		tenant, err := svc.CreateTenant(context.Background(), &domain.Tenant{
			Name:          "acme",
			TenantGroupID: "group-1",
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if tenant.ID == "" {
			t.Error("expected tenant to have an ID")
		}
	})

	t.Run("defaults to default tenant_group_id", func(t *testing.T) {
		tenantRepo.EXPECT().
			Create(gomock.Any(), gomock.Any()).
			DoAndReturn(func(_ context.Context, tenant *domain.Tenant) error {
				if tenant.TenantGroupID != service.DefaultTenantGroupID {
					t.Errorf("expected tenant_group_id %s, got %s", service.DefaultTenantGroupID, tenant.TenantGroupID)
				}
				return nil
			})

		_, err := svc.CreateTenant(context.Background(), &domain.Tenant{
			Name: "acme",
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})
}

func TestGetTenantGroup(t *testing.T) {
	ctrl := gomock.NewController(t)
	tenantGroupRepo := mocks.NewMockTenantGroupRepository(ctrl)
	tenantRepo := mocks.NewMockTenantRepository(ctrl)
	modelPricingRepo := mocks.NewMockModelPricingRepository(ctrl)
	svc := service.NewRegistrationService(tenantGroupRepo, tenantRepo, modelPricingRepo)

	tenantGroupRepo.EXPECT().
		Get(gomock.Any(), "group-1").
		Return(&domain.TenantGroup{ID: "group-1", Name: "production"}, nil)

	group, err := svc.GetTenantGroup(context.Background(), "group-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if group.Name != "production" {
		t.Errorf("expected name production, got %s", group.Name)
	}
}

func TestGetTenant(t *testing.T) {
	ctrl := gomock.NewController(t)
	tenantGroupRepo := mocks.NewMockTenantGroupRepository(ctrl)
	tenantRepo := mocks.NewMockTenantRepository(ctrl)
	modelPricingRepo := mocks.NewMockModelPricingRepository(ctrl)
	svc := service.NewRegistrationService(tenantGroupRepo, tenantRepo, modelPricingRepo)

	tenantRepo.EXPECT().
		Get(gomock.Any(), "tenant-1").
		Return(&domain.Tenant{ID: "tenant-1", Name: "acme"}, nil)

	tenant, err := svc.GetTenant(context.Background(), "tenant-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if tenant.Name != "acme" {
		t.Errorf("expected name acme, got %s", tenant.Name)
	}
}
