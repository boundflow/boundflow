package postgres

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/boundflow/boundflow/internal/domain"
)

type TenantRepo struct {
	pool *pgxpool.Pool
}

func NewTenantRepo(pool *pgxpool.Pool) *TenantRepo {
	return &TenantRepo{pool: pool}
}

func (r *TenantRepo) Create(ctx context.Context, tenant *domain.Tenant) error {
	var overridesJSON []byte
	if tenant.PolicyOverrides != nil {
		var err error
		overridesJSON, err = json.Marshal(tenant.PolicyOverrides)
		if err != nil {
			return fmt.Errorf("marshal policy overrides: %w", err)
		}
	}

	_, err := r.pool.Exec(ctx,
		`INSERT INTO tenants (id, tenant_group_id, name, policy_overrides, created_at)
		 VALUES ($1, $2, $3, $4, $5)`,
		tenant.ID, tenant.TenantGroupID, tenant.Name, overridesJSON, tenant.CreatedAt,
	)
	if err != nil {
		return handleError(err, "tenant")
	}
	return nil
}

func (r *TenantRepo) Get(ctx context.Context, id string) (*domain.Tenant, error) {
	var tenant domain.Tenant
	var overridesJSON []byte

	err := r.pool.QueryRow(ctx,
		`SELECT id, tenant_group_id, name, policy_overrides, created_at
		 FROM tenants WHERE id = $1`, id,
	).Scan(&tenant.ID, &tenant.TenantGroupID, &tenant.Name, &overridesJSON, &tenant.CreatedAt)
	if err != nil {
		return nil, handleError(err, "tenant")
	}

	if overridesJSON != nil {
		tenant.PolicyOverrides = &domain.PolicySet{}
		if err := json.Unmarshal(overridesJSON, tenant.PolicyOverrides); err != nil {
			return nil, fmt.Errorf("unmarshal policy overrides: %w", err)
		}
	}

	return &tenant, nil
}

func (r *TenantRepo) Delete(ctx context.Context, id string) error {
	_, err := r.pool.Exec(ctx, `DELETE FROM tenants WHERE id = $1`, id)
	if err != nil {
		return fmt.Errorf("delete tenant: %w", err)
	}
	return nil
}
