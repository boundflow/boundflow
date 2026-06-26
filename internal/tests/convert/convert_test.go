package convert_test

import (
	"testing"

	boundflowv1 "github.com/boundflow/boundflow/gen/boundflow/v1"
	"github.com/boundflow/boundflow/internal/convert"
	"github.com/boundflow/boundflow/internal/domain"
)

func TestTenantGroupFromProto(t *testing.T) {
	t.Run("valid", func(t *testing.T) {
		group, err := convert.TenantGroupFromProto(&boundflowv1.TenantGroup{Name: "prod"})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if group.Name != "prod" {
			t.Errorf("expected name prod, got %s", group.Name)
		}
	})

	t.Run("nil", func(t *testing.T) {
		_, err := convert.TenantGroupFromProto(nil)
		if err == nil {
			t.Fatal("expected error for nil")
		}
	})

	t.Run("missing name", func(t *testing.T) {
		_, err := convert.TenantGroupFromProto(&boundflowv1.TenantGroup{})
		if err == nil {
			t.Fatal("expected error for missing name")
		}
	})
}

func TestTenantGroupToProto(t *testing.T) {
	group := &domain.TenantGroup{
		ID:   "group-1",
		Name: "production",
	}
	pb := convert.TenantGroupToProto(group)
	if pb.Id != "group-1" {
		t.Errorf("expected id group-1, got %s", pb.Id)
	}
	if pb.Name != "production" {
		t.Errorf("expected name production, got %s", pb.Name)
	}
}

func TestTenantFromProto(t *testing.T) {
	t.Run("valid with tenant_group_id", func(t *testing.T) {
		gid := "group-1"
		tenant, err := convert.TenantFromProto(&boundflowv1.Tenant{
			Name:          "acme",
			TenantGroupId: &gid,
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if tenant.Name != "acme" {
			t.Errorf("expected name acme, got %s", tenant.Name)
		}
		if tenant.TenantGroupID != "group-1" {
			t.Errorf("expected tenant_group_id group-1, got %s", tenant.TenantGroupID)
		}
	})

	t.Run("valid without tenant_group_id", func(t *testing.T) {
		tenant, err := convert.TenantFromProto(&boundflowv1.Tenant{Name: "acme"})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if tenant.TenantGroupID != "" {
			t.Errorf("expected empty tenant_group_id, got %s", tenant.TenantGroupID)
		}
	})

	t.Run("nil", func(t *testing.T) {
		_, err := convert.TenantFromProto(nil)
		if err == nil {
			t.Fatal("expected error for nil")
		}
	})

	t.Run("missing name", func(t *testing.T) {
		_, err := convert.TenantFromProto(&boundflowv1.Tenant{})
		if err == nil {
			t.Fatal("expected error for missing name")
		}
	})
}

func TestTenantToProto(t *testing.T) {
	tenant := &domain.Tenant{
		ID:            "tenant-1",
		Name:          "acme",
		TenantGroupID: "group-1",
	}
	pb := convert.TenantToProto(tenant)
	if pb.Id != "tenant-1" {
		t.Errorf("expected id tenant-1, got %s", pb.Id)
	}
	if pb.Name != "acme" {
		t.Errorf("expected name acme, got %s", pb.Name)
	}
	if pb.TenantGroupId == nil || *pb.TenantGroupId != "group-1" {
		t.Errorf("expected tenant_group_id group-1, got %v", pb.TenantGroupId)
	}
}
