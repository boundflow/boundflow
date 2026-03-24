package postgres

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/convergeplane/convergeplane/internal/domain"
)

type TenantGroupRepo struct {
	pool *pgxpool.Pool
}

func NewTenantGroupRepo(pool *pgxpool.Pool) *TenantGroupRepo {
	return &TenantGroupRepo{pool: pool}
}

func (r *TenantGroupRepo) Create(ctx context.Context, group *domain.TenantGroup) error {
	policies, err := json.Marshal(group.Policies)
	if err != nil {
		return fmt.Errorf("marshal policies: %w", err)
	}

	_, err = r.pool.Exec(ctx,
		`INSERT INTO tenant_groups (id, name, policies, created_at)
		 VALUES ($1, $2, $3, $4)`,
		group.ID, group.Name, policies, group.CreatedAt,
	)
	if err != nil {
		return fmt.Errorf("insert tenant group: %w", err)
	}
	return nil
}

func (r *TenantGroupRepo) Get(ctx context.Context, id string) (*domain.TenantGroup, error) {
	var group domain.TenantGroup
	var policiesJSON []byte

	err := r.pool.QueryRow(ctx,
		`SELECT id, name, policies, created_at
		 FROM tenant_groups WHERE id = $1`, id,
	).Scan(&group.ID, &group.Name, &policiesJSON, &group.CreatedAt)
	if err != nil {
		return nil, fmt.Errorf("get tenant group: %w", err)
	}

	if err := json.Unmarshal(policiesJSON, &group.Policies); err != nil {
		return nil, fmt.Errorf("unmarshal policies: %w", err)
	}

	return &group, nil
}
