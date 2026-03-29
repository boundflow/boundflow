package convert

import (
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	convergeplanev1 "github.com/convergeplane/convergeplane/gen/convergeplane/v1"
	"github.com/convergeplane/convergeplane/internal/domain"
)

func ResourceStateFromProto(s *structpb.Struct) domain.ResourceState {
	if s == nil {
		return nil
	}
	return domain.ResourceState(s.AsMap())
}

func ResourceStateToProto(s domain.ResourceState) *structpb.Struct {
	if s == nil {
		return nil
	}
	pb, _ := structpb.NewStruct(s)
	return pb
}

func ResourceInstanceToProto(r *domain.ResourceInstance) *convergeplanev1.ResourceInstance {
	if r == nil {
		return nil
	}
	return &convergeplanev1.ResourceInstance{
		Id:           r.ID,
		TenantId:     r.TenantID,
		CurrentState: ResourceStateToProto(r.CurrentConfigState),
		GoalState:    ResourceStateToProto(r.ConfigGoalState),
		CreatedAt:    timestamppb.New(r.CreatedAt),
	}
}
