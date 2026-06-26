package domain

// ModelPricing is the per-1M-token rate for a model, in USD.
type ModelPricing struct {
	ModelID     string
	InputPer1M  float64
	OutputPer1M float64
}
