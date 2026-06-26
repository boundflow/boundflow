package convert

import (
	"fmt"

	"google.golang.org/protobuf/types/known/timestamppb"

	boundflowv1 "github.com/boundflow/boundflow/gen/boundflow/v1"
	"github.com/boundflow/boundflow/internal/domain"
)

func TenantGroupFromProto(pb *boundflowv1.TenantGroup) (*domain.TenantGroup, error) {
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

func TenantGroupToProto(g *domain.TenantGroup) *boundflowv1.TenantGroup {
	return &boundflowv1.TenantGroup{
		Id:        g.ID,
		Name:      g.Name,
		Policies:  PolicySetToProto(g.Policies),
		CreatedAt: timestamppb.New(g.CreatedAt),
	}
}
