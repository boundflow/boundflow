package convert

import (
	"fmt"

	"google.golang.org/protobuf/types/known/timestamppb"

	convergeplanev1 "github.com/convergeplane/convergeplane/gen/convergeplane/v1"
	"github.com/convergeplane/convergeplane/internal/domain"
)

func TenantFromProto(pb *convergeplanev1.Tenant) (*domain.Tenant, error) {
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

func TenantToProto(t *domain.Tenant) *convergeplanev1.Tenant {
	pb := &convergeplanev1.Tenant{
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
