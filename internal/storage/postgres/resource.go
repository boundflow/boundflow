package postgres

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/convergeplane/convergeplane/internal/domain"
)

type ResourceRepo struct {
	pool *pgxpool.Pool
}

func NewResourceRepo(pool *pgxpool.Pool) *ResourceRepo {
	return &ResourceRepo{pool: pool}
}

func (r *ResourceRepo) Create(ctx context.Context, resource *domain.Resource) error {
	currentState, err := json.Marshal(resource.CurrentState)
	if err != nil {
		return fmt.Errorf("marshal current state: %w", err)
	}

	goalState, err := json.Marshal(resource.GoalState)
	if err != nil {
		return fmt.Errorf("marshal goal state: %w", err)
	}

	_, err = r.pool.Exec(ctx,
		`INSERT INTO resources (id, tenant_id, resource_type, current_state, goal_state, created_at, updated_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		resource.ID, resource.TenantID, resource.ResourceType,
		currentState, goalState,
		resource.CreatedAt, resource.UpdatedAt,
	)
	if err != nil {
		return fmt.Errorf("insert resource: %w", err)
	}
	return nil
}

func (r *ResourceRepo) Get(ctx context.Context, tenantID, id string) (*domain.Resource, error) {
	var resource domain.Resource
	var currentStateJSON, goalStateJSON []byte

	err := r.pool.QueryRow(ctx,
		`SELECT id, tenant_id, resource_type, current_state, goal_state, created_at, updated_at
		 FROM resources WHERE tenant_id = $1 AND id = $2`,
		tenantID, id,
	).Scan(
		&resource.ID, &resource.TenantID, &resource.ResourceType,
		&currentStateJSON, &goalStateJSON,
		&resource.CreatedAt, &resource.UpdatedAt,
	)
	if err != nil {
		return nil, fmt.Errorf("get resource: %w", err)
	}

	if err := json.Unmarshal(currentStateJSON, &resource.CurrentState); err != nil {
		return nil, fmt.Errorf("unmarshal current state: %w", err)
	}
	if err := json.Unmarshal(goalStateJSON, &resource.GoalState); err != nil {
		return nil, fmt.Errorf("unmarshal goal state: %w", err)
	}

	return &resource, nil
}

func (r *ResourceRepo) Update(ctx context.Context, resource *domain.Resource) error {
	currentState, err := json.Marshal(resource.CurrentState)
	if err != nil {
		return fmt.Errorf("marshal current state: %w", err)
	}

	goalState, err := json.Marshal(resource.GoalState)
	if err != nil {
		return fmt.Errorf("marshal goal state: %w", err)
	}

	_, err = r.pool.Exec(ctx,
		`UPDATE resources
		 SET resource_type = $1, current_state = $2, goal_state = $3, updated_at = $4
		 WHERE tenant_id = $5 AND id = $6`,
		resource.ResourceType, currentState, goalState, resource.UpdatedAt,
		resource.TenantID, resource.ID,
	)
	if err != nil {
		return fmt.Errorf("update resource: %w", err)
	}
	return nil
}
