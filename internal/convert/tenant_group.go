package convert

import (
	"fmt"

	"google.golang.org/protobuf/types/known/timestamppb"

	convergeplanev1 "github.com/convergeplane/convergeplane/gen/convergeplane/v1"
	"github.com/convergeplane/convergeplane/internal/domain"
)

func TenantGroupFromProto(pb *convergeplanev1.TenantGroup) (*domain.TenantGroup, error) {
	if pb == nil {
		return nil, fmt.Errorf("tenant_group is required")
	}
	if pb.Name == "" {
		return nil, fmt.Errorf("name is required")
	}

	return &domain.TenantGroup{
		Name:     pb.Name,
		Policies: PolicySetFromProto(pb.Policies),
	}, nil
}

func TenantGroupToProto(g *domain.TenantGroup) *convergeplanev1.TenantGroup {
	return &convergeplanev1.TenantGroup{
		Id:        g.ID,
		Name:      g.Name,
		Policies:  PolicySetToProto(g.Policies),
		CreatedAt: timestamppb.New(g.CreatedAt),
	}
}
