package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/convergeplane/convergeplane/internal/domain"
	"github.com/convergeplane/convergeplane/internal/storage"
)

type ResourceInstanceRepo struct {
	pool *pgxpool.Pool
}

func NewResourceInstanceRepo(pool *pgxpool.Pool) *ResourceInstanceRepo {
	return &ResourceInstanceRepo{pool: pool}
}

func (r *ResourceInstanceRepo) Create(ctx context.Context, instance *domain.ResourceInstance) error {
	lifecyclePolicyJSON, err := json.Marshal(instance.LifecyclePolicy)
	if err != nil {
		return fmt.Errorf("marshal lifecycle policy: %w", err)
	}
	_, err = r.pool.Exec(ctx,
		`INSERT INTO resource_instances
		   (id, tenant_id, resource_type,
		    current_workflow_version, invoke_timeout_seconds, repeat_every_seconds, triggerable,
		    lifecycle_state, workflow_state, lifecycle_policy, scheduler_partition_id,
		    last_completed_request_at, created_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)`,
		instance.ID, instance.TenantID, instance.ResourceType,
		instance.CurrentWorkflowVersion,
		instance.WorkflowConfig.InvokeTimeoutSeconds,
		instance.WorkflowConfig.RepeatEverySeconds,
		instance.WorkflowConfig.Triggerable,
		instance.LifecycleState, instance.WorkflowState, lifecyclePolicyJSON, instance.SchedulerPartitionID,
		instance.LastCompletedRequestAt, instance.CreatedAt,
	)
	if err != nil {
		return handleError(err, "resource instance")
	}
	return nil
}

func (r *ResourceInstanceRepo) Get(ctx context.Context, id string) (*domain.ResourceInstance, error) {
	var instance domain.ResourceInstance
	var lifecyclePolicyJSON, invocationMetricsJSON []byte

	err := r.pool.QueryRow(ctx,
		`SELECT id, tenant_id, resource_type,
		        invoke_timeout_seconds, repeat_every_seconds, triggerable,
		        lifecycle_state, workflow_state, lifecycle_policy, invocation_metrics, cooldown_until,
		        lifecycle_last_resolved, current_workflow_version, scheduler_partition_id,
		        target_version, current_version, last_completed_request_at, created_at
		 FROM resource_instances WHERE id = $1`, id,
	).Scan(
		&instance.ID, &instance.TenantID, &instance.ResourceType,
		&instance.WorkflowConfig.InvokeTimeoutSeconds,
		&instance.WorkflowConfig.RepeatEverySeconds,
		&instance.WorkflowConfig.Triggerable,
		&instance.LifecycleState, &instance.WorkflowState,
		&lifecyclePolicyJSON, &invocationMetricsJSON, &instance.CooldownUntil,
		&instance.LifecycleLastResolved, &instance.CurrentWorkflowVersion, &instance.SchedulerPartitionID,
		&instance.TargetVersion, &instance.CurrentVersion,
		&instance.LastCompletedRequestAt, &instance.CreatedAt,
	)
	if err != nil {
		return nil, handleError(err, "resource instance")
	}

	if err := json.Unmarshal(lifecyclePolicyJSON, &instance.LifecyclePolicy); err != nil {
		return nil, fmt.Errorf("unmarshal lifecycle_policy: %w", err)
	}
	if err := json.Unmarshal(invocationMetricsJSON, &instance.InvocationMetrics); err != nil {
		return nil, fmt.Errorf("unmarshal invocation_metrics: %w", err)
	}
	sort.Slice(instance.InvocationMetrics, func(i, j int) bool {
		return instance.InvocationMetrics[i].RanAt < instance.InvocationMetrics[j].RanAt
	})

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

func (r *ResourceInstanceRepo) UpdateLifecyclePolicy(ctx context.Context, id string, policy domain.WorkflowLifecyclePolicy) error {
	data, err := json.Marshal(policy)
	if err != nil {
		return fmt.Errorf("marshal lifecycle policy: %w", err)
	}
	_, err = r.pool.Exec(ctx,
		`UPDATE resource_instances SET lifecycle_policy = $1 WHERE id = $2`,
		data, id,
	)
	if err != nil {
		return fmt.Errorf("update lifecycle policy: %w", err)
	}
	return nil
}

func (r *ResourceInstanceRepo) MarkDeleted(ctx context.Context, id string) error {
	_, err := r.pool.Exec(ctx,
		`UPDATE resource_instances SET lifecycle_state = $1, workflow_state = $2 WHERE id = $3`,
		domain.LifecycleStateDeleted, domain.WorkflowStateDisabled, id,
	)
	if err != nil {
		return fmt.Errorf("mark deleted: %w", err)
	}
	return nil
}

func (r *ResourceInstanceRepo) UpdateWorkflowState(ctx context.Context, id string, state domain.WorkflowState) error {
	_, err := r.pool.Exec(ctx,
		`UPDATE resource_instances SET workflow_state = $1 WHERE id = $2`,
		state, id,
	)
	if err != nil {
		return fmt.Errorf("update workflow state: %w", err)
	}
	return nil
}

func (r *ResourceInstanceRepo) UpdateLifecycleStateAndIncrementVersion(ctx context.Context, id string, state domain.LifecycleState, invalidStates ...domain.LifecycleState) (int64, error) {
	invalid := make([]string, len(invalidStates))
	for i, s := range invalidStates {
		invalid[i] = string(s)
	}

	var currentLifecycleState domain.LifecycleState
	var newVersion *int64
	err := r.pool.QueryRow(ctx,
		`WITH current AS (
		   SELECT lifecycle_state FROM resource_instances WHERE id = $1
		 ), updated AS (
		   UPDATE resource_instances
		   SET lifecycle_state = $2, target_version = target_version + 1
		   WHERE id = $1 AND NOT (lifecycle_state = ANY($3::lifecycle_state[]))
		   RETURNING target_version
		 )
		 SELECT current.lifecycle_state, updated.target_version
		 FROM current LEFT JOIN updated ON true`,
		id, state, invalid,
	).Scan(&currentLifecycleState, &newVersion)
	if err != nil {
		return 0, handleError(err, "resource instance")
	}
	if newVersion == nil {
		return 0, fmt.Errorf("%w: resource is %s", storage.ErrInvalidLifecycleState, currentLifecycleState)
	}
	return *newVersion, nil
}

func (r *ResourceInstanceRepo) StartInvocationAndIncrementVersion(ctx context.Context, id string, invalidStates ...domain.LifecycleState) (int64, error) {
	invalid := make([]string, len(invalidStates))
	for i, s := range invalidStates {
		invalid[i] = string(s)
	}

	var currentLifecycleState domain.LifecycleState
	var newVersion *int64
	err := r.pool.QueryRow(ctx,
		`WITH current AS (
		   SELECT lifecycle_state FROM resource_instances WHERE id = $1
		 ), updated AS (
		   UPDATE resource_instances
		   SET lifecycle_state = $2, target_version = target_version + 1
		   WHERE id = $1 AND NOT (lifecycle_state = ANY($3::lifecycle_state[]))
		   RETURNING target_version
		 )
		 SELECT current.lifecycle_state, updated.target_version
		 FROM current LEFT JOIN updated ON true`,
		id, domain.LifecycleStateReconciling, invalid,
	).Scan(&currentLifecycleState, &newVersion)
	if err != nil {
		return 0, handleError(err, "resource instance")
	}
	if newVersion == nil {
		return 0, fmt.Errorf("%w: resource is %s", storage.ErrInvalidLifecycleState, currentLifecycleState)
	}
	return *newVersion, nil
}

func (r *ResourceInstanceRepo) IncrementTargetVersion(ctx context.Context, id string) (int64, error) {
	var newVersion int64
	err := r.pool.QueryRow(ctx,
		`UPDATE resource_instances SET target_version = target_version + 1 WHERE id = $1 RETURNING target_version`,
		id,
	).Scan(&newVersion)
	if err != nil {
		return 0, fmt.Errorf("increment target version: %w", err)
	}
	return newVersion, nil
}

func (r *ResourceInstanceRepo) UpdateCurrentVersion(ctx context.Context, id string, version int64) error {
	_, err := r.pool.Exec(ctx,
		`UPDATE resource_instances SET current_version = $1 WHERE id = $2`,
		version, id,
	)
	if err != nil {
		return fmt.Errorf("update current version: %w", err)
	}
	return nil
}

func (r *ResourceInstanceRepo) ApplyCompletedJob(ctx context.Context, id string, lifecycleState domain.LifecycleState, version int64) (bool, error) {
	var updatedID string
	err := r.pool.QueryRow(ctx,
		`UPDATE resource_instances
		 SET current_version = $3,
		     last_completed_request_at = now(),
		     lifecycle_state = CASE WHEN target_version = $3 THEN $2::lifecycle_state ELSE lifecycle_state END
		 WHERE id = $1 AND current_version < $3
		 RETURNING id`,
		id, lifecycleState, version,
	).Scan(&updatedID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, nil
		}
		return false, fmt.Errorf("apply completed job: %w", err)
	}
	return true, nil
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

func (r *ResourceInstanceRepo) TenantGroupIDForResource(ctx context.Context, resourceInstanceID string) (string, error) {
	var groupID string
	err := r.pool.QueryRow(ctx,
		`SELECT t.tenant_group_id
		 FROM resource_instances ri
		 JOIN tenants t ON t.id = ri.tenant_id
		 WHERE ri.id = $1 AND ri.deleted_at IS NULL`,
		resourceInstanceID,
	).Scan(&groupID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", storage.ErrNotFound
		}
		return "", fmt.Errorf("tenant group for resource: %w", err)
	}
	return groupID, nil
}
