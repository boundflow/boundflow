package domain

// ResourceState represents the customer-defined configuration state of a resource.
// This is opaque to the platform — customers define whatever properties
// describe their resource's state (e.g. {"sku": "v2", "region": "west"}).
type ResourceState map[string]any
