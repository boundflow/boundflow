package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
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
		`INSERT INTO tenants (id, tenant_group_id, name, policy_overrides, created_at, scheduler_partition_id)
		 VALUES ($1, $2, $3, $4, $5, $6)`,
		tenant.ID, tenant.TenantGroupID, tenant.Name, overridesJSON, tenant.CreatedAt, tenant.SchedulerPartitionID,
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
		`SELECT id, tenant_group_id, name, policy_overrides, created_at, deleted_at, scheduler_partition_id
		 FROM tenants WHERE id = $1`, id,
	).Scan(&tenant.ID, &tenant.TenantGroupID, &tenant.Name, &overridesJSON, &tenant.CreatedAt, &tenant.DeletedAt, &tenant.SchedulerPartitionID)
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
		`SELECT id, tenant_group_id, name, policy_overrides, created_at, deleted_at, scheduler_partition_id
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
			&tenant.ID, &tenant.TenantGroupID, &tenant.Name, &overridesJSON, &tenant.CreatedAt, &tenant.DeletedAt, &tenant.SchedulerPartitionID,
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

// MarkDeleted soft-deletes the tenant. Guarded on workflow_count = 0, and this UPDATE is
// the serialization point that prevents a concurrent WorkflowRepo.Create/FinalizeDeleted
// from racing an in-flight tenant delete (see workflow.go). A guard failure (tenant
// missing, already deleted, or still has workflows) can't be distinguished from a single
// statement, so it's reported uniformly as ErrTenantHasWorkflows.
func (r *TenantRepo) MarkDeleted(ctx context.Context, id string) error {
	var updatedID string
	err := r.pool.QueryRow(ctx,
		`UPDATE tenants SET deleted_at = now()
		 WHERE id = $1 AND deleted_at IS NULL AND workflow_count = 0
		 RETURNING id`,
		id,
	).Scan(&updatedID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return storage.ErrTenantHasWorkflows
		}
		return fmt.Errorf("mark tenant deleted: %w", err)
	}
	return nil
}

// PurgeIfEmpty hard-deletes the tenant row, but only if it's soft-deleted and no rows
// remain in workflows for it (ground truth, independent of workflow_count).
func (r *TenantRepo) PurgeIfEmpty(ctx context.Context, id string) (bool, error) {
	tag, err := r.pool.Exec(ctx,
		`DELETE FROM tenants
		 WHERE id = $1 AND deleted_at IS NOT NULL
		   AND NOT EXISTS (SELECT 1 FROM workflows WHERE tenant_id = $1)`,
		id,
	)
	if err != nil {
		return false, fmt.Errorf("purge tenant: %w", err)
	}
	return tag.RowsAffected() > 0, nil
}

// ListPurgeable returns soft-deleted tenant IDs in the partition. MarkDeleted only sets
// deleted_at when workflow_count = 0, and nothing can increment it afterward (Create is
// guarded on deleted_at IS NULL), so workflow_count is always 0 once a tenant is
// soft-deleted — no need to filter on it here.
func (r *TenantRepo) ListPurgeable(ctx context.Context, partitionID string) ([]string, error) {
	rows, err := r.pool.Query(ctx,
		`SELECT id FROM tenants WHERE scheduler_partition_id = $1 AND deleted_at IS NOT NULL`,
		partitionID,
	)
	if err != nil {
		return nil, fmt.Errorf("list purgeable tenants: %w", err)
	}
	defer rows.Close()

	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan purgeable tenant id: %w", err)
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}
