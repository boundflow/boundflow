// Package pricing produces a tenant group's effective model rates by merging
// the operator-managed global defaults with the tenant's own overrides. Both
// sets live in the database (default_model_pricing / model_pricing); this package
// only does the merge.
package pricing

import "github.com/convergeplane/convergeplane/internal/domain"

// Effective merges a tenant group's overrides over the global defaults, keyed by
// model ID. An override replaces a default for the same model; a model that only
// has an override (no default) is included as-is.
func Effective(defaults, overrides []domain.ModelPricing) map[string]domain.ModelPricing {
	out := make(map[string]domain.ModelPricing, len(defaults)+len(overrides))
	for _, d := range defaults {
		out[d.ModelID] = d
	}
	for _, o := range overrides {
		out[o.ModelID] = o
	}
	return out
}
