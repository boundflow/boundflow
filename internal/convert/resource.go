package convert

import (
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/convergeplane/convergeplane/internal/domain"
)

func ResourceStateFromProto(s *structpb.Struct) domain.ResourceState {
	if s == nil {
		return nil
	}
	return domain.ResourceState(s.AsMap())
}
