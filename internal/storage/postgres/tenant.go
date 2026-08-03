package postgres

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/boundflow/boundflow/internal/domain"
	"github.com/boundflow/boundflow/internal/storage"
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

func (r *TenantRepo) ListForTenantGroup(ctx context.Context, tenantGroupID string) ([]*domain.Tenant, error) {
	rows, err := r.pool.Query(ctx,
		`SELECT id, tenant_group_id, name, policy_overrides, created_at
		 FROM tenants WHERE tenant_group_id = $1
		 ORDER BY created_at DESC`, tenantGroupID,
	)
	if err != nil {
		return nil, handleError(err, "tenant")
	}
	defer rows.Close()

	var out []*domain.Tenant
	for rows.Next() {
		var tenant domain.Tenant
		var overridesJSON []byte
		if err := rows.Scan(
			&tenant.ID, &tenant.TenantGroupID, &tenant.Name, &overridesJSON, &tenant.CreatedAt,
		); err != nil {
			return nil, handleError(err, "tenant")
		}
		if overridesJSON != nil {
			tenant.PolicyOverrides = &domain.PolicySet{}
			if err := json.Unmarshal(overridesJSON, tenant.PolicyOverrides); err != nil {
				return nil, fmt.Errorf("unmarshal policy overrides: %w", err)
			}
		}
		out = append(out, &tenant)
	}
	if err := rows.Err(); err != nil {
		return nil, handleError(err, "tenant")
	}
	return out, nil
}

// Delete removes a tenant and purges the operational rows of its workflows. Workflow
// deletion is a permanent soft-delete (lifecycle_state = 'deleted'), so those rows
// would otherwise hold the tenant's foreign key forever; this drops them once they are
// deleted. Audit events carry no foreign key and are intentionally left intact, so the
// record of what ran survives the tenant. Refuses while any workflow is still live.
func (r *TenantRepo) Delete(ctx context.Context, id string) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("delete tenant: %w", err)
	}
	defer tx.Rollback(ctx)

	var live int
	err = tx.QueryRow(ctx,
		`SELECT count(*) FROM workflows WHERE tenant_id = $1 AND lifecycle_state != $2`,
		id, domain.LifecycleStateDeleted,
	).Scan(&live)
	if err != nil {
		return fmt.Errorf("count live workflows: %w", err)
	}
	if live > 0 {
		return fmt.Errorf("%w: %d remaining", storage.ErrTenantHasWorkflows, live)
	}

	// Ordered children-first; agent_states cascades with the workflow row.
	for _, q := range []string{
		`DELETE FROM jobs WHERE workflow_id IN (SELECT id FROM workflows WHERE tenant_id = $1)`,
		`DELETE FROM customer_requests WHERE workflow_id IN (SELECT id FROM workflows WHERE tenant_id = $1)`,
		`DELETE FROM workflow_version_metrics WHERE workflow_id IN (SELECT id FROM workflows WHERE tenant_id = $1)`,
		`DELETE FROM workflows WHERE tenant_id = $1`,
	} {
		if _, err := tx.Exec(ctx, q, id); err != nil {
			return fmt.Errorf("purge tenant workflows: %w", err)
		}
	}

	tag, err := tx.Exec(ctx, `DELETE FROM tenants WHERE id = $1`, id)
	if err != nil {
		return fmt.Errorf("delete tenant: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return storage.ErrNotFound
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("delete tenant: %w", err)
	}
	return nil
}
