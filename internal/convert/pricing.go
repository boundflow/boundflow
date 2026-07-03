package convert

import (
	"fmt"

	boundflowv1 "github.com/boundflow/boundflow/gen/boundflow/v1"
	"github.com/boundflow/boundflow/internal/domain"
)

func ModelPricingFromProto(pb *boundflowv1.ModelPricing) (domain.ModelPricing, error) {
	if pb == nil {
		return domain.ModelPricing{}, fmt.Errorf("pricing is required")
	}
	if pb.ModelId == "" {
		return domain.ModelPricing{}, fmt.Errorf("model_id is required")
	}
	if pb.InputPer_1M < 0 || pb.OutputPer_1M < 0 {
		return domain.ModelPricing{}, fmt.Errorf("rates must be non-negative")
	}
	return domain.ModelPricing{
		ModelID:     pb.ModelId,
		InputPer1M:  pb.InputPer_1M,
		OutputPer1M: pb.OutputPer_1M,
	}, nil
}

func ModelPricingToProto(p domain.ModelPricing) *boundflowv1.ModelPricing {
	return &boundflowv1.ModelPricing{
		ModelId:      p.ModelID,
		InputPer_1M:  p.InputPer1M,
		OutputPer_1M: p.OutputPer1M,
	}
}
