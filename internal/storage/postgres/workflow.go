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

	"github.com/boundflow/boundflow/internal/domain"
	"github.com/boundflow/boundflow/internal/storage"
)

type WorkflowRepo struct {
	pool *pgxpool.Pool
}

func NewWorkflowRepo(pool *pgxpool.Pool) *WorkflowRepo {
	return &WorkflowRepo{pool: pool}
}

func (r *WorkflowRepo) Create(ctx context.Context, instance *domain.Workflow) error {
	lifecyclePolicyJSON, err := json.Marshal(instance.LifecyclePolicy)
	if err != nil {
		return fmt.Errorf("marshal lifecycle policy: %w", err)
	}
	_, err = r.pool.Exec(ctx,
		`INSERT INTO workflows
		   (id, tenant_id, workflow_type,
		    current_workflow_version, invoke_timeout_seconds, repeat_every_seconds, triggerable,
		    invoke_mode, max_queue_depth,
		    lifecycle_state, workflow_state, lifecycle_policy, scheduler_partition_id,
		    last_completed_request_at, created_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)`,
		instance.ID, instance.TenantID, instance.WorkflowType,
		instance.CurrentWorkflowVersion,
		instance.WorkflowConfig.InvokeTimeoutSeconds,
		instance.WorkflowConfig.RepeatEverySeconds,
		instance.WorkflowConfig.Triggerable,
		string(instance.WorkflowConfig.InvokeMode), instance.WorkflowConfig.MaxQueueDepth,
		instance.Lifecycle.State, instance.WorkflowState, lifecyclePolicyJSON, instance.SchedulerPartitionID,
		instance.Lifecycle.LastCompletedRequestAt, instance.CreatedAt,
	)
	if err != nil {
		return handleError(err, "workflow instance")
	}
	return nil
}

// ListForTenantGroup returns a lightweight view of every workflow owned by the
// given tenant group (via the workflows→tenants join), newest first. Only the
// dashboard-relevant columns are populated; heavy fields (policy, metrics) are
// left zero — fetch a single workflow with Get for the full record.
func (r *WorkflowRepo) ListForTenantGroup(ctx context.Context, tenantGroupID string) ([]*domain.Workflow, error) {
	rows, err := r.pool.Query(ctx,
		`SELECT w.id, w.tenant_id, w.workflow_type, w.current_workflow_version,
		        w.lifecycle_state, w.workflow_state, w.last_completed_request_at,
		        w.last_interrupted_request_id, w.created_at
		 FROM workflows w
		 JOIN tenants t ON w.tenant_id = t.id
		 WHERE t.tenant_group_id = $1
		 ORDER BY w.created_at DESC`, tenantGroupID,
	)
	if err != nil {
		return nil, handleError(err, "workflow instance")
	}
	defer rows.Close()

	var out []*domain.Workflow
	for rows.Next() {
		var w domain.Workflow
		if err := rows.Scan(
			&w.ID, &w.TenantID, &w.WorkflowType, &w.CurrentWorkflowVersion,
			&w.Lifecycle.State, &w.WorkflowState, &w.Lifecycle.LastCompletedRequestAt,
			&w.Lifecycle.LastInterruptedRequestID, &w.CreatedAt,
		); err != nil {
			return nil, handleError(err, "workflow instance")
		}
		out = append(out, &w)
	}
	if err := rows.Err(); err != nil {
		return nil, handleError(err, "workflow instance")
	}
	return out, nil
}

func (r *WorkflowRepo) Get(ctx context.Context, id string) (*domain.Workflow, error) {
	var instance domain.Workflow
	var lifecyclePolicyJSON, invocationMetricsJSON []byte
	var invokeMode string
	var gateMetadataJSON []byte
	var gateDetail *string

	err := r.pool.QueryRow(ctx,
		`SELECT w.id, w.tenant_id, w.workflow_type,
		        w.invoke_timeout_seconds, w.repeat_every_seconds, w.triggerable, w.invoke_mode, w.max_queue_depth,
		        w.lifecycle_state, w.workflow_state, w.lifecycle_policy, w.invocation_metrics, w.cooldown_until,
		        w.lifecycle_last_resolved, w.current_workflow_version, w.scheduler_partition_id,
		        w.target_version, w.current_version, w.last_completed_request_at,
		        w.last_interrupted_request_id, w.created_at,
		        w.last_gate_id, w.last_gate_detail, w.last_gate_metadata,
		        w.last_gate_opened_at, w.last_gate_timeout_at
		 FROM workflows w
		 WHERE w.id = $1`, id,
	).Scan(
		&instance.ID, &instance.TenantID, &instance.WorkflowType,
		&instance.WorkflowConfig.InvokeTimeoutSeconds,
		&instance.WorkflowConfig.RepeatEverySeconds,
		&instance.WorkflowConfig.Triggerable,
		&invokeMode, &instance.WorkflowConfig.MaxQueueDepth,
		&instance.Lifecycle.State, &instance.WorkflowState,
		&lifecyclePolicyJSON, &invocationMetricsJSON, &instance.CooldownUntil,
		&instance.LifecycleLastResolved, &instance.CurrentWorkflowVersion, &instance.SchedulerPartitionID,
		&instance.TargetVersion, &instance.CurrentVersion,
		&instance.Lifecycle.LastCompletedRequestAt, &instance.Lifecycle.LastInterruptedRequestID, &instance.CreatedAt,
		&instance.Lifecycle.LastGateID, &gateDetail, &gateMetadataJSON,
		&instance.Lifecycle.LastGateOpenedAt, &instance.Lifecycle.LastGateTimeoutAt,
	)
	if err != nil {
		return nil, handleError(err, "workflow instance")
	}
	instance.WorkflowConfig.InvokeMode = domain.InvokeMode(invokeMode)
	if gateDetail != nil {
		instance.Lifecycle.LastGateDetail = *gateDetail
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
	if gateMetadataJSON != nil {
		if err := json.Unmarshal(gateMetadataJSON, &instance.Lifecycle.LastGateMetadata); err != nil {
			return nil, fmt.Errorf("unmarshal last gate metadata: %w", err)
		}
	}

	return &instance, nil
}

func (r *WorkflowRepo) UpdateLifecycleState(ctx context.Context, id string, state domain.LifecycleState) error {
	_, err := r.pool.Exec(ctx,
		`UPDATE workflows SET lifecycle_state = $1 WHERE id = $2`,
		state, id,
	)
	if err != nil {
		return fmt.Errorf("update lifecycle state: %w", err)
	}
	return nil
}

func (r *WorkflowRepo) UpdateLifecyclePolicy(ctx context.Context, id string, policy domain.WorkflowLifecyclePolicy) error {
	data, err := json.Marshal(policy)
	if err != nil {
		return fmt.Errorf("marshal lifecycle policy: %w", err)
	}
	_, err = r.pool.Exec(ctx,
		`UPDATE workflows SET lifecycle_policy = $1 WHERE id = $2`,
		data, id,
	)
	if err != nil {
		return fmt.Errorf("update lifecycle policy: %w", err)
	}
	return nil
}

func (r *WorkflowRepo) UpdateConfig(ctx context.Context, id string, cfg domain.WorkflowConfig, version int) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin update config tx: %w", err)
	}
	defer tx.Rollback(ctx)

	var oldVersion int
	if err := tx.QueryRow(ctx,
		`SELECT current_workflow_version FROM workflows WHERE id = $1 FOR UPDATE`, id,
	).Scan(&oldVersion); err != nil {
		return fmt.Errorf("lock workflow for config update: %w", err)
	}

	if _, err := tx.Exec(ctx,
		`UPDATE workflows
		   SET current_workflow_version = $1, invoke_timeout_seconds = $2, repeat_every_seconds = $3,
		       triggerable = $4, invoke_mode = $5, max_queue_depth = $6
		 WHERE id = $7`,
		version, cfg.InvokeTimeoutSeconds, cfg.RepeatEverySeconds, cfg.Triggerable,
		string(cfg.InvokeMode), cfg.MaxQueueDepth, id,
	); err != nil {
		return fmt.Errorf("update workflow config: %w", err)
	}

	if oldVersion != version {
		if err := startNewMetricsEpoch(ctx, tx, id, version); err != nil {
			return err
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit update config tx: %w", err)
	}
	return nil
}

// startNewMetricsEpoch starts a fresh (zeroed) workflow_version_metrics row for a
// workflow that just transitioned to the given version, so its cumulative totals
// begin from zero rather than resuming a stale streak from a prior time on this
// version. Must be called within the same transaction as the version change.
func startNewMetricsEpoch(ctx context.Context, tx pgx.Tx, workflowID string, version int) error {
	_, err := tx.Exec(ctx,
		`INSERT INTO workflow_version_metrics (workflow_id, version, epoch)
		 SELECT $1, $2, COALESCE(MAX(epoch), 0) + 1
		 FROM workflow_version_metrics WHERE workflow_id = $1 AND version = $2`,
		workflowID, version,
	)
	if err != nil {
		return fmt.Errorf("start new metrics epoch: %w", err)
	}
	return nil
}

func (r *WorkflowRepo) MarkDeleted(ctx context.Context, id string) error {
	_, err := r.pool.Exec(ctx,
		`UPDATE workflows SET lifecycle_state = $1, workflow_state = $2 WHERE id = $3`,
		domain.LifecycleStateDeleted, domain.WorkflowStateDisabled, id,
	)
	if err != nil {
		return fmt.Errorf("mark deleted: %w", err)
	}
	return nil
}

func (r *WorkflowRepo) UpdateWorkflowState(ctx context.Context, id string, state domain.WorkflowState) error {
	_, err := r.pool.Exec(ctx,
		`UPDATE workflows SET workflow_state = $1 WHERE id = $2`,
		state, id,
	)
	if err != nil {
		return fmt.Errorf("update workflow state: %w", err)
	}
	return nil
}

func (r *WorkflowRepo) UpdateLifecycleStateAndIncrementVersion(ctx context.Context, id string, state domain.LifecycleState, invalidStates ...domain.LifecycleState) (int64, error) {
	invalid := make([]string, len(invalidStates))
	for i, s := range invalidStates {
		invalid[i] = string(s)
	}

	var currentLifecycleState domain.LifecycleState
	var newVersion *int64
	err := r.pool.QueryRow(ctx,
		`WITH current AS (
		   SELECT lifecycle_state FROM workflows WHERE id = $1
		 ), updated AS (
		   UPDATE workflows
		   SET lifecycle_state = $2, target_version = target_version + 1
		   WHERE id = $1 AND NOT (lifecycle_state = ANY($3::lifecycle_state[]))
		   RETURNING target_version
		 )
		 SELECT current.lifecycle_state, updated.target_version
		 FROM current LEFT JOIN updated ON true`,
		id, state, invalid,
	).Scan(&currentLifecycleState, &newVersion)
	if err != nil {
		return 0, handleError(err, "workflow instance")
	}
	if newVersion == nil {
		return 0, fmt.Errorf("%w: workflow is %s", storage.ErrInvalidLifecycleState, currentLifecycleState)
	}
	return *newVersion, nil
}

func (r *WorkflowRepo) StartInvocationAndIncrementVersion(ctx context.Context, id string, invalidStates ...domain.LifecycleState) (int64, error) {
	invalid := make([]string, len(invalidStates))
	for i, s := range invalidStates {
		invalid[i] = string(s)
	}

	var currentLifecycleState domain.LifecycleState
	var newVersion *int64
	err := r.pool.QueryRow(ctx,
		`WITH current AS (
		   SELECT lifecycle_state FROM workflows WHERE id = $1
		 ), updated AS (
		   UPDATE workflows
		   SET lifecycle_state = $2, target_version = target_version + 1
		   WHERE id = $1 AND NOT (lifecycle_state = ANY($3::lifecycle_state[]))
		   RETURNING target_version
		 )
		 SELECT current.lifecycle_state, updated.target_version
		 FROM current LEFT JOIN updated ON true`,
		id, domain.LifecycleStateInvoking, invalid,
	).Scan(&currentLifecycleState, &newVersion)
	if err != nil {
		return 0, handleError(err, "workflow instance")
	}
	if newVersion == nil {
		return 0, fmt.Errorf("%w: workflow is %s", storage.ErrInvalidLifecycleState, currentLifecycleState)
	}
	return *newVersion, nil
}

func (r *WorkflowRepo) IncrementTargetVersion(ctx context.Context, id string) (int64, error) {
	var newVersion int64
	err := r.pool.QueryRow(ctx,
		`UPDATE workflows SET target_version = target_version + 1 WHERE id = $1 RETURNING target_version`,
		id,
	).Scan(&newVersion)
	if err != nil {
		return 0, fmt.Errorf("increment target version: %w", err)
	}
	return newVersion, nil
}

func (r *WorkflowRepo) UpdateCurrentVersion(ctx context.Context, id string, version int64) error {
	_, err := r.pool.Exec(ctx,
		`UPDATE workflows SET current_version = $1 WHERE id = $2`,
		version, id,
	)
	if err != nil {
		return fmt.Errorf("update current version: %w", err)
	}
	return nil
}

func (r *WorkflowRepo) ApplyCompletedJob(ctx context.Context, id string, lifecycleState domain.LifecycleState, version int64) (bool, error) {
	var updatedID string
	err := r.pool.QueryRow(ctx,
		`UPDATE workflows
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

func (r *WorkflowRepo) ApplyFailedJob(ctx context.Context, id string, requestID string, lifecycleState domain.LifecycleState, workflowState domain.WorkflowState, version int64) (bool, error) {
	var updatedID string
	err := r.pool.QueryRow(ctx,
		`UPDATE workflows
		 SET current_version = $5,
		     last_completed_request_at = now(),
		     last_interrupted_request_id = $2,
		     lifecycle_state = $3::lifecycle_state,
		     workflow_state  = $4::workflow_state
		 WHERE id = $1 AND current_version < $5
		 RETURNING id`,
		id, requestID, lifecycleState, workflowState, version,
	).Scan(&updatedID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, nil
		}
		return false, fmt.Errorf("apply failed job: %w", err)
	}
	return true, nil
}

func (r *WorkflowRepo) ResolveInterruptedWorkflow(ctx context.Context, id string, requestID string) (bool, error) {
	var updatedID string
	err := r.pool.QueryRow(ctx,
		`UPDATE workflows
		 SET lifecycle_state = 'active', workflow_state = 'active'
		 WHERE id = $1 AND lifecycle_state = 'interrupted' AND last_interrupted_request_id = $2
		 RETURNING id`,
		id, requestID,
	).Scan(&updatedID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, nil
		}
		return false, fmt.Errorf("resolve interrupted workflow: %w", err)
	}
	return true, nil
}

func (r *WorkflowRepo) UpdateSchedulerPartition(ctx context.Context, id string, partitionID string) error {
	_, err := r.pool.Exec(ctx,
		`UPDATE workflows SET scheduler_partition_id = $1 WHERE id = $2`,
		partitionID, id,
	)
	if err != nil {
		return fmt.Errorf("update scheduler partition: %w", err)
	}
	return nil
}

func (r *WorkflowRepo) UpdateLastCompletedRequestAt(ctx context.Context, id string, t time.Time) error {
	_, err := r.pool.Exec(ctx,
		`UPDATE workflows SET last_completed_request_at = $1 WHERE id = $2`,
		t, id,
	)
	if err != nil {
		return fmt.Errorf("update last completed request at: %w", err)
	}
	return nil
}

func (r *WorkflowRepo) TenantGroupIDForWorkflow(ctx context.Context, workflowID string) (string, error) {
	var groupID string
	err := r.pool.QueryRow(ctx,
		`SELECT t.tenant_group_id
		 FROM workflows ri
		 JOIN tenants t ON t.id = ri.tenant_id
		 WHERE ri.id = $1 AND ri.lifecycle_state != 'deleted'`,
		workflowID,
	).Scan(&groupID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", storage.ErrNotFound
		}
		return "", fmt.Errorf("tenant group for workflow: %w", err)
	}
	return groupID, nil
}
