package convert

import (
	"fmt"

	"google.golang.org/protobuf/types/known/timestamppb"

	boundflowv1 "github.com/boundflow/boundflow/gen/boundflow/v1"
	"github.com/boundflow/boundflow/internal/domain"
)

func TenantFromProto(pb *boundflowv1.Tenant) (*domain.Tenant, error) {
	if pb == nil {
		return nil, fmt.Errorf("tenant is required")
	}
	if pb.Name == "" {
		return nil, fmt.Errorf("name is required")
	}

	t := &domain.Tenant{
		Name: pb.Name,
	}

	if pb.TenantGroupId != nil {
		t.TenantGroupID = *pb.TenantGroupId
	}

	if pb.PolicyOverrides != nil {
		ps := PolicySetFromProto(pb.PolicyOverrides)
		t.PolicyOverrides = &ps
	}

	return t, nil
}

func TenantToProto(t *domain.Tenant) *boundflowv1.Tenant {
	pb := &boundflowv1.Tenant{
		Id:        t.ID,
		Name:      t.Name,
		CreatedAt: timestamppb.New(t.CreatedAt),
	}

	if t.TenantGroupID != "" {
		pb.TenantGroupId = &t.TenantGroupID
	}

	if t.PolicyOverrides != nil {
		pb.PolicyOverrides = PolicySetToProto(*t.PolicyOverrides)
	}

	return pb
}
