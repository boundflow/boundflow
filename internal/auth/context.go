package auth

import "context"

type contextKey int

const tenantGroupKey contextKey = iota

// WithTenantGroup stores the resolved tenant group ID in the context.
func WithTenantGroup(ctx context.Context, tenantGroupID string) context.Context {
	return context.WithValue(ctx, tenantGroupKey, tenantGroupID)
}

// TenantGroupFromContext returns the tenant group ID injected by the auth interceptor.
// Returns ("", false) if the context carries no identity — should not happen in
// authenticated handlers, but callers can decide how to handle it.
func TenantGroupFromContext(ctx context.Context) (string, bool) {
	id, ok := ctx.Value(tenantGroupKey).(string)
	return id, ok && id != ""
}
