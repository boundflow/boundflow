package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/convergeplane/convergeplane/internal/domain"
)

type ResourceInstanceRepo struct {
	pool *pgxpool.Pool
}

func NewResourceInstanceRepo(pool *pgxpool.Pool) *ResourceInstanceRepo {
	return &ResourceInstanceRepo{pool: pool}
}

func (r *ResourceInstanceRepo) Create(ctx context.Context, instance *domain.ResourceInstance) error {
	currentState, err := json.Marshal(instance.CurrentConfigState)
	if err != nil {
		return fmt.Errorf("marshal current config state: %w", err)
	}

	goalState, err := json.Marshal(instance.ConfigGoalState)
	if err != nil {
		return fmt.Errorf("marshal config goal state: %w", err)
	}

	_, err = r.pool.Exec(ctx,
		`INSERT INTO resource_instances (id, tenant_id, resource_type, current_config_state, config_goal_state, lifecycle_state, scheduler_partition_id, version, last_completed_request_at, created_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
		instance.ID, instance.TenantID, instance.ResourceType,
		currentState, goalState,
		instance.LifecycleState, instance.SchedulerPartitionID,
		instance.Version, instance.LastCompletedRequestAt, instance.CreatedAt,
	)
	if err != nil {
		return handleError(err, "resource instance")
	}
	return nil
}

func (r *ResourceInstanceRepo) Get(ctx context.Context, id string) (*domain.ResourceInstance, error) {
	var instance domain.ResourceInstance
	var currentStateJSON, goalStateJSON []byte

	err := r.pool.QueryRow(ctx,
		`SELECT id, tenant_id, resource_type, current_config_state, config_goal_state, lifecycle_state, scheduler_partition_id, version, last_completed_request_at, created_at
		 FROM resource_instances WHERE id = $1`, id,
	).Scan(
		&instance.ID, &instance.TenantID, &instance.ResourceType,
		&currentStateJSON, &goalStateJSON,
		&instance.LifecycleState, &instance.SchedulerPartitionID,
		&instance.Version, &instance.LastCompletedRequestAt, &instance.CreatedAt,
	)
	if err != nil {
		return nil, handleError(err, "resource instance")
	}

	if err := json.Unmarshal(currentStateJSON, &instance.CurrentConfigState); err != nil {
		return nil, fmt.Errorf("unmarshal current config state: %w", err)
	}
	if err := json.Unmarshal(goalStateJSON, &instance.ConfigGoalState); err != nil {
		return nil, fmt.Errorf("unmarshal config goal state: %w", err)
	}

	return &instance, nil
}

func (r *ResourceInstanceRepo) UpdateLifecycleState(ctx context.Context, id string, state domain.LifecycleState) error {
	_, err := r.pool.Exec(ctx,
		`UPDATE resource_instances SET lifecycle_state = $1 WHERE id = $2`,
		state, id,
	)
	if err != nil {
		return fmt.Errorf("update lifecycle state: %w", err)
	}
	return nil
}

func (r *ResourceInstanceRepo) UpdateLifecycleStateAndIncrementVersion(ctx context.Context, id string, state domain.LifecycleState) (int64, error) {
	var newVersion int64
	err := r.pool.QueryRow(ctx,
		`UPDATE resource_instances SET lifecycle_state = $1, version = version + 1 WHERE id = $2 RETURNING version`,
		state, id,
	).Scan(&newVersion)
	if err != nil {
		return 0, handleError(err, "resource instance")
	}
	return newVersion, nil
}

func (r *ResourceInstanceRepo) UpdateConfigState(ctx context.Context, id string, currentState, goalState domain.ResourceState) error {
	currentJSON, err := json.Marshal(currentState)
	if err != nil {
		return fmt.Errorf("marshal current config state: %w", err)
	}

	goalJSON, err := json.Marshal(goalState)
	if err != nil {
		return fmt.Errorf("marshal config goal state: %w", err)
	}

	_, err = r.pool.Exec(ctx,
		`UPDATE resource_instances SET current_config_state = $1, config_goal_state = $2 WHERE id = $3`,
		currentJSON, goalJSON, id,
	)
	if err != nil {
		return fmt.Errorf("update config state: %w", err)
	}
	return nil
}

func (r *ResourceInstanceRepo) UpdateGoalStateAndIncrementVersion(ctx context.Context, id string, goalState domain.ResourceState) (int64, error) {
	goalJSON, err := json.Marshal(goalState)
	if err != nil {
		return 0, fmt.Errorf("marshal goal state: %w", err)
	}

	var newVersion int64
	err = r.pool.QueryRow(ctx,
		`UPDATE resource_instances SET config_goal_state = $1, version = version + 1 WHERE id = $2 RETURNING version`,
		goalJSON, id,
	).Scan(&newVersion)
	if err != nil {
		return 0, handleError(err, "resource instance")
	}
	return newVersion, nil
}

func (r *ResourceInstanceRepo) IncrementVersion(ctx context.Context, id string) (int64, error) {
	var newVersion int64
	err := r.pool.QueryRow(ctx,
		`UPDATE resource_instances SET version = version + 1 WHERE id = $1 RETURNING version`,
		id,
	).Scan(&newVersion)
	if err != nil {
		return 0, fmt.Errorf("increment version: %w", err)
	}
	return newVersion, nil
}

func (r *ResourceInstanceRepo) UpdateSchedulerPartition(ctx context.Context, id string, partitionID string) error {
	_, err := r.pool.Exec(ctx,
		`UPDATE resource_instances SET scheduler_partition_id = $1 WHERE id = $2`,
		partitionID, id,
	)
	if err != nil {
		return fmt.Errorf("update scheduler partition: %w", err)
	}
	return nil
}

func (r *ResourceInstanceRepo) UpdateLastCompletedRequestAt(ctx context.Context, id string, t time.Time) error {
	_, err := r.pool.Exec(ctx,
		`UPDATE resource_instances SET last_completed_request_at = $1 WHERE id = $2`,
		t, id,
	)
	if err != nil {
		return fmt.Errorf("update last completed request at: %w", err)
	}
	return nil
}
