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
)

type LifecycleResolverRepo struct {
	pool *pgxpool.Pool
}

func NewLifecycleResolverRepo(pool *pgxpool.Pool) *LifecycleResolverRepo {
	return &LifecycleResolverRepo{pool: pool}
}

func (r *LifecycleResolverRepo) GetExpiredCooldownResources(ctx context.Context, partitionID string) ([]*domain.ResourceInstance, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT id, tenant_id, resource_type,
		       invoke_timeout_seconds, repeat_every_seconds, triggerable,
		       lifecycle_state, workflow_state, lifecycle_policy, invocation_metrics, cooldown_until,
		       lifecycle_last_resolved, current_workflow_version, scheduler_partition_id,
		       target_version, current_version, last_completed_request_at, created_at
		FROM resource_instances
		WHERE scheduler_partition_id = $1
		  AND workflow_state = 'cooldown'
		  AND cooldown_until <= now()
	`, partitionID)
	if err != nil {
		return nil, fmt.Errorf("query expired cooldown resources: %w", err)
	}
	defer rows.Close()

	var instances []*domain.ResourceInstance
	for rows.Next() {
		var inst domain.ResourceInstance
		var lifecyclePolicyJSON, invocationMetricsJSON []byte
		if err := rows.Scan(
			&inst.ID, &inst.TenantID, &inst.ResourceType,
			&inst.WorkflowConfig.InvokeTimeoutSeconds,
			&inst.WorkflowConfig.RepeatEverySeconds,
			&inst.WorkflowConfig.Triggerable,
			&inst.LifecycleState, &inst.WorkflowState,
			&lifecyclePolicyJSON, &invocationMetricsJSON, &inst.CooldownUntil,
			&inst.LifecycleLastResolved, &inst.CurrentWorkflowVersion, &inst.SchedulerPartitionID,
			&inst.TargetVersion, &inst.CurrentVersion,
			&inst.LastCompletedRequestAt, &inst.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan resource instance: %w", err)
		}
		if err := json.Unmarshal(lifecyclePolicyJSON, &inst.LifecyclePolicy); err != nil {
			return nil, fmt.Errorf("unmarshal lifecycle_policy: %w", err)
		}
		if err := json.Unmarshal(invocationMetricsJSON, &inst.InvocationMetrics); err != nil {
			return nil, fmt.Errorf("unmarshal invocation_metrics: %w", err)
		}
		sort.Slice(inst.InvocationMetrics, func(i, j int) bool {
			return inst.InvocationMetrics[i].RanAt < inst.InvocationMetrics[j].RanAt
		})
		instances = append(instances, &inst)
	}
	return instances, rows.Err()
}

// ExpireCooldowns flips every cooldown workflow in the partition whose cooldown has elapsed
// back to active in a single atomic statement. The WHERE clause is the guard: a row that is
// no longer in cooldown (or not yet expired) at write time is skipped. Returns the IDs resumed.
func (r *LifecycleResolverRepo) ExpireCooldowns(ctx context.Context, partitionID string) ([]string, error) {
	rows, err := r.pool.Query(ctx, `
		UPDATE resource_instances
		SET workflow_state = 'active',
		    cooldown_until = NULL
		WHERE scheduler_partition_id = $1
		  AND workflow_state = 'cooldown'
		  AND cooldown_until <= now()
		RETURNING id
	`, partitionID)
	if err != nil {
		return nil, fmt.Errorf("expire cooldowns: %w", err)
	}
	defer rows.Close()

	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan resumed id: %w", err)
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

func (r *LifecycleResolverRepo) TryApplyPolicyResolution(ctx context.Context, resourceInstanceID string, resolved int64, workflowVersion int, workflowState domain.WorkflowState, cooldownUntil *time.Time) (bool, error) {
	var updatedID string
	err := r.pool.QueryRow(ctx, `
		UPDATE resource_instances
		SET lifecycle_last_resolved  = $2,
		    current_workflow_version = $3,
		    workflow_state           = $4,
		    cooldown_until           = $5
		WHERE id = $1
		  AND lifecycle_last_resolved < $2
		RETURNING id
	`, resourceInstanceID, resolved, workflowVersion, workflowState, cooldownUntil).Scan(&updatedID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, nil
		}
		return false, fmt.Errorf("try apply policy resolution: %w", err)
	}
	return true, nil
}
