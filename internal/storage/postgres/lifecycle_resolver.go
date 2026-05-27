package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/convergeplane/convergeplane/internal/domain"
)

// lifecycleResolverLockClass is the advisory lock class for lifecycle resolver locks.
// Each lock type gets a unique class so keys can never collide across different lock purposes.
const lifecycleResolverLockClass = 1001

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
		       current_workflow_version, scheduler_partition_id,
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
			&inst.CurrentWorkflowVersion, &inst.SchedulerPartitionID,
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
		instances = append(instances, &inst)
	}
	return instances, rows.Err()
}

func (r *LifecycleResolverRepo) TryWithResolverLock(ctx context.Context, resourceInstanceID string, fn func() error) (bool, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("begin resolver lock transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	var acquired bool
	if err := tx.QueryRow(ctx,
		`SELECT pg_try_advisory_xact_lock($1, hashtext($2))`,
		lifecycleResolverLockClass, resourceInstanceID,
	).Scan(&acquired); err != nil {
		return false, fmt.Errorf("try advisory xact lock: %w", err)
	}

	if !acquired {
		return false, nil
	}

	if err := fn(); err != nil {
		return true, err
	}

	if err := tx.Commit(ctx); err != nil {
		return true, fmt.Errorf("commit resolver lock transaction: %w", err)
	}
	return true, nil
}

func (r *LifecycleResolverRepo) GetCurrentVersionMetrics(ctx context.Context, resourceInstanceID string, version int) (*domain.WorkflowVersionMetrics, error) {
	var m domain.WorkflowVersionMetrics
	var toolFailureCountsRaw []byte

	err := r.pool.QueryRow(ctx, `
		SELECT resource_instance_id, version, epoch,
		       total_cost, run_count, total_failures, total_llm_calls,
		       total_latency_seconds, total_approval_rejections, tool_failure_counts
		FROM workflow_version_metrics
		WHERE resource_instance_id = $1 AND version = $2
		ORDER BY epoch DESC
		LIMIT 1
	`, resourceInstanceID, version).Scan(
		&m.ResourceInstanceID, &m.Version, &m.Epoch,
		&m.TotalCost, &m.RunCount, &m.TotalFailures, &m.TotalLLMCalls,
		&m.TotalLatencySeconds, &m.TotalApprovalRejections, &toolFailureCountsRaw,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("query current version metrics: %w", err)
	}

	if err := json.Unmarshal(toolFailureCountsRaw, &m.ToolFailureCounts); err != nil {
		return nil, fmt.Errorf("unmarshal tool_failure_counts: %w", err)
	}

	return &m, nil
}
