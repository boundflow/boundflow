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

	tag, err := r.pool.Exec(ctx,
		`WITH reserved AS (
		     UPDATE tenants SET workflow_count = workflow_count + 1
		     WHERE id = $1 AND deleted_at IS NULL
		     RETURNING id
		 )
		 INSERT INTO workflows
		   (id, tenant_id, workflow_type,
		    current_workflow_version, invoke_timeout_seconds, repeat_every_seconds, triggerable,
		    invoke_mode, max_queue_depth,
		    lifecycle_state, workflow_state, lifecycle_policy, scheduler_partition_id,
		    last_completed_request_at, created_at)
		 SELECT $2, $1, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15
		 WHERE EXISTS (SELECT 1 FROM reserved)`,
		instance.TenantID, instance.ID, instance.WorkflowType,
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
	if tag.RowsAffected() == 0 {
		var deletedAt *time.Time
		lookupErr := r.pool.QueryRow(ctx, `SELECT deleted_at FROM tenants WHERE id = $1`, instance.TenantID).Scan(&deletedAt)
		if errors.Is(lookupErr, pgx.ErrNoRows) {
			return storage.ErrNotFound
		}
		if lookupErr != nil {
			return fmt.Errorf("lookup tenant for create workflow: %w", lookupErr)
		}
		return storage.ErrTenantDeleted
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
		        w.last_interrupted_request_id, w.created_at, w.deletion_requested_at,
		        w.last_policy_decision_request_id
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
			&w.Lifecycle.LastInterruptedRequestID, &w.CreatedAt, &w.DeletionRequestedAt,
			&w.LastPolicyDecisionRequestID,
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
	var suspensionID *string

	err := r.pool.QueryRow(ctx,
		`SELECT w.id, w.tenant_id, w.workflow_type,
		        w.invoke_timeout_seconds, w.repeat_every_seconds, w.triggerable, w.invoke_mode, w.max_queue_depth,
		        w.lifecycle_state, w.workflow_state, w.lifecycle_policy, w.invocation_metrics, w.cooldown_until,
		        w.lifecycle_last_resolved, w.current_workflow_version, w.scheduler_partition_id,
		        w.target_version, w.current_version, w.last_completed_request_at,
		        w.last_interrupted_request_id, w.created_at,
		        w.last_gate_id, w.last_gate_detail, w.last_gate_metadata,
		        w.last_gate_opened_at, w.last_gate_timeout_at, w.deletion_requested_at,
		        w.last_policy_decision_request_id,
		        w.suspension_id, w.suspension_reason, w.suspension_stop_current,
		        w.suspension_abandon_queued, w.suspension_requested_at, w.suspension_finalized_at
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
		&instance.Lifecycle.LastGateOpenedAt, &instance.Lifecycle.LastGateTimeoutAt, &instance.DeletionRequestedAt,
		&instance.LastPolicyDecisionRequestID,
		&suspensionID, &instance.Suspension.Reason, &instance.Suspension.StopCurrent,
		&instance.Suspension.AbandonQueued, &instance.Suspension.RequestedAt,
		&instance.Suspension.FinalizedAt,
	)
	if err != nil {
		return nil, handleError(err, "workflow instance")
	}
	instance.WorkflowConfig.InvokeMode = domain.InvokeMode(invokeMode)
	if suspensionID != nil {
		instance.Suspension.ID = *suspensionID
	}
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

// startNewMetricsEpoch starts a fresh workflow_version_metrics row for the given
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

// MarkDeletionRequested disables the workflow for new work and records when deletion
// was requested. Idle work still in flight is left alone; AbandonUnscheduledRequests
// and FinalizeDeleted (called after this, and again periodically by the reconciler)
// bring the workflow the rest of the way to lifecycle_state = deleted. Fails with
// ErrDeletionAlreadyRequested if deletion was already requested for this workflow.
func (r *WorkflowRepo) MarkDeletionRequested(ctx context.Context, id string) error {
	var updatedID string
	err := r.pool.QueryRow(ctx,
		`UPDATE workflows SET workflow_state = $1, deletion_requested_at = now()
		 WHERE id = $2 AND deletion_requested_at IS NULL
		 RETURNING id`,
		domain.WorkflowStateDisabled, id,
	).Scan(&updatedID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return storage.ErrDeletionAlreadyRequested
		}
		return fmt.Errorf("mark deletion requested: %w", err)
	}
	return nil
}

// MarkSuspensionRequested records an operator's suspension: the workflow stops being
// runnable immediately, and the tail — freezing queued work, stopping a live run,
// finalizing — is finished inline or by the reconciler. suspension_requested_at set
// with suspension_finalized_at still NULL is what the suspend reconciler picks up as
// draining; lifecycle_state = 'resuming' is the separate flag a resume sets later.
func (r *WorkflowRepo) MarkSuspensionRequested(ctx context.Context, id, suspensionID, reason string, stopCurrent, abandonQueued bool) error {
	var updatedID string
	err := r.pool.QueryRow(ctx,
		`UPDATE workflows
		 SET workflow_state = $2, suspension_requested_at = now(), suspension_id = $3,
		     suspension_reason = $4, suspension_stop_current = $5, suspension_abandon_queued = $6
		 WHERE id = $1
		   AND suspension_requested_at IS NULL
		   AND workflow_state NOT IN ('disabled', 'suspended')
		 RETURNING id`,
		id, domain.WorkflowStateSuspended, suspensionID, reason, stopCurrent, abandonQueued,
	).Scan(&updatedID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return storage.ErrSuspensionAlreadyRequested
		}
		return fmt.Errorf("mark suspension requested: %w", err)
	}
	return nil
}

// FinalizeSuspended records that the suspension has taken effect: nothing is running
// any more, so the workflow is halted rather than merely held. Call only once
// CustomerRequestRepo.HasRunningRequest reports nothing in flight.
//
// This is what permits a resume — the workflow_state has said 'suspended' since the
// request landed, so it cannot tell draining from drained; the finalized timestamp can.
//
// Guarded on a suspension actually being in flight, and on not having finalized already
// so the reconciler's re-runs keep the original timestamp.
func (r *WorkflowRepo) FinalizeSuspended(ctx context.Context, id string) error {
	_, err := r.pool.Exec(ctx,
		`UPDATE workflows
		 SET lifecycle_state = $2, suspension_finalized_at = now()
		 WHERE id = $1
		   AND suspension_requested_at IS NOT NULL
		   AND suspension_finalized_at IS NULL`,
		id, domain.LifecycleStateHalted,
	)
	if err != nil {
		return fmt.Errorf("finalize suspended: %w", err)
	}
	return nil
}

// FinalizeDeleted marks the workflow deleted and decrements its tenant's workflow_count
// in one statement. Only call after CustomerRequestRepo.HasRunningRequest reports nothing
// running. Idempotent: a workflow already lifecycle_state = deleted is a no-op.
func (r *WorkflowRepo) FinalizeDeleted(ctx context.Context, id string) error {
	_, err := r.pool.Exec(ctx,
		`WITH deleted_workflow AS (
		     UPDATE workflows SET lifecycle_state = $1, deletion_finalized_at = now()
		     WHERE id = $2 AND lifecycle_state != $1
		     RETURNING tenant_id
		 )
		 UPDATE tenants SET workflow_count = workflow_count - 1
		 WHERE id = (SELECT tenant_id FROM deleted_workflow) AND workflow_count > 0`,
		domain.LifecycleStateDeleted, id,
	)
	if err != nil {
		return fmt.Errorf("finalize deleted: %w", err)
	}
	return nil
}

// PurgeDeleted deletes the workflows row itself. Only call once every child table
// (customer_requests, workflow_version_metrics; agent_state cascades automatically) has
// already been cleared for this workflow by WorkflowPurgeReconciler.
func (r *WorkflowRepo) PurgeDeleted(ctx context.Context, id string) (bool, error) {
	tag, err := r.pool.Exec(ctx,
		`DELETE FROM workflows WHERE id = $1 AND lifecycle_state = $2`,
		id, domain.LifecycleStateDeleted,
	)
	if err != nil {
		return false, fmt.Errorf("purge workflow: %w", err)
	}
	return tag.RowsAffected() > 0, nil
}

// ListPurgeable returns finalized (lifecycle_state = deleted) workflow IDs in the
// partition whose deletion_finalized_at is older than olderThan.
func (r *WorkflowRepo) ListPurgeable(ctx context.Context, partitionID string, olderThan time.Duration) ([]string, error) {
	rows, err := r.pool.Query(ctx,
		`SELECT id FROM workflows
		 WHERE scheduler_partition_id = $1
		   AND lifecycle_state = $2
		   AND deletion_finalized_at < now() - ($3 * interval '1 second')`,
		partitionID, domain.LifecycleStateDeleted, olderThan.Seconds(),
	)
	if err != nil {
		return nil, fmt.Errorf("list purgeable workflows: %w", err)
	}
	defer rows.Close()

	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan purgeable workflow id: %w", err)
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

// ListPendingDeletion returns IDs of workflows in the partition where deletion has been
// requested but not yet finalized - stragglers for the periodic reconciler to retry.
// ListPendingSuspension returns the workflows in the partition whose suspension has
// been requested but has not taken effect yet — the ones still draining. Full rows
// rather than ids, since finishing one needs the operator's choices off the row.
func (r *WorkflowRepo) ListPendingSuspension(ctx context.Context, partitionID string) ([]*domain.Workflow, error) {
	rows, err := r.pool.Query(ctx,
		`SELECT id, suspension_id, suspension_reason, suspension_stop_current,
		        suspension_abandon_queued, suspension_requested_at, suspension_finalized_at
		 FROM workflows
		 WHERE scheduler_partition_id = $1
		   AND suspension_requested_at IS NOT NULL
		   AND suspension_finalized_at IS NULL`,
		partitionID,
	)
	if err != nil {
		return nil, fmt.Errorf("list pending suspension: %w", err)
	}
	defer rows.Close()

	var out []*domain.Workflow
	for rows.Next() {
		var w domain.Workflow
		var suspensionID *string
		if err := rows.Scan(
			&w.ID, &suspensionID, &w.Suspension.Reason, &w.Suspension.StopCurrent,
			&w.Suspension.AbandonQueued, &w.Suspension.RequestedAt, &w.Suspension.FinalizedAt,
		); err != nil {
			return nil, fmt.Errorf("scan pending suspension: %w", err)
		}
		if suspensionID != nil {
			w.Suspension.ID = *suspensionID
		}
		out = append(out, &w)
	}
	return out, rows.Err()
}

func (r *WorkflowRepo) ListPendingDeletion(ctx context.Context, partitionID string) ([]string, error) {
	rows, err := r.pool.Query(ctx,
		`SELECT id FROM workflows
		 WHERE scheduler_partition_id = $1
		   AND deletion_requested_at IS NOT NULL
		   AND lifecycle_state != $2`,
		partitionID, domain.LifecycleStateDeleted,
	)
	if err != nil {
		return nil, fmt.Errorf("list pending deletion: %w", err)
	}
	defer rows.Close()

	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan pending deletion id: %w", err)
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

// TryActivateWorkflow resumes a paused/cooldown workflow, guarded on requestID
// matching last_policy_decision_request_id. disabled is never touched here —
// that's ResolveInterruptedWorkflow's guard.
func (r *WorkflowRepo) TryActivateWorkflow(ctx context.Context, id string, requestID string) (bool, error) {
	var updatedID string
	err := r.pool.QueryRow(ctx,
		`UPDATE workflows
		 SET workflow_state = 'active', cooldown_until = NULL
		 WHERE id = $1
		   AND workflow_state IN ('paused', 'cooldown')
		   AND last_policy_decision_request_id = $2
		 RETURNING id`,
		id, requestID,
	).Scan(&updatedID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, nil
		}
		return false, fmt.Errorf("try activate workflow: %w", err)
	}
	return true, nil
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

// MarkResumeRequested starts a resume: lifecycle_state is the sole flag the reconciler
// uses to recognize a resume in progress, so a row can be picked back up and finished
// after a crash without losing suspension_requested_at — the final restore step still
// needs it (to compute how much of a cooldown was left when the suspension froze it).
//
// The workflow stays unrunnable until the last step restores workflow_state, so the
// unfreezing in between can't let anything schedule early.
//
// Guarded on the suspension having finalized: a suspension still draining isn't
// resumable, since the two reconcilers would otherwise be working the same workflow
// from opposite ends. Guarded on suspensionID so a caller can't clear a suspension it
// never saw. Returns false when either guard fails.
func (r *WorkflowRepo) MarkResumeRequested(ctx context.Context, id string, suspensionID string) (bool, error) {
	var updatedID string
	err := r.pool.QueryRow(ctx,
		`UPDATE workflows
		 SET lifecycle_state = $3
		 WHERE id = $1
		   AND suspension_id = $2
		   AND workflow_state = 'suspended'
		   AND suspension_finalized_at IS NOT NULL
		 RETURNING id`,
		id, suspensionID, domain.LifecycleStateResuming,
	).Scan(&updatedID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, nil
		}
		return false, fmt.Errorf("mark resume requested: %w", err)
	}
	return true, nil
}

// RestoreFromSuspension finishes a resume: workflow_state lands on the caller's recomputed
// target rather than unconditionally 'active', since the lifecycle policy may have
// independently been holding the workflow in paused/cooldown right up until the suspension.
// lifecycle_state goes back to active and every suspension column is cleared.
//
// Guarded on lifecycle_state = 'resuming' (the flag MarkResumeRequested set) and on
// suspensionID, so a retry after a crash is safe and a stale caller can't clobber a
// newer suspension.
func (r *WorkflowRepo) RestoreFromSuspension(ctx context.Context, id, suspensionID string, state domain.WorkflowState, cooldownUntil *time.Time) error {
	_, err := r.pool.Exec(ctx,
		`UPDATE workflows
		 SET workflow_state = $3, cooldown_until = $4, lifecycle_state = 'active',
		     suspension_requested_at = NULL, suspension_finalized_at = NULL,
		     suspension_id = NULL, suspension_reason = '',
		     suspension_stop_current = false, suspension_abandon_queued = false
		 WHERE id = $1
		   AND suspension_id = $2
		   AND lifecycle_state = 'resuming'`,
		id, suspensionID, state, cooldownUntil,
	)
	if err != nil {
		return fmt.Errorf("restore from suspension: %w", err)
	}
	return nil
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

// TenantGroupIDForWorkflow resolves ownership regardless of lifecycle state, including
// soft-deleted (lifecycle_state = deleted) workflows — a soft-deleted workflow should
// still be readable by its owner until it's actually purged.
func (r *WorkflowRepo) TenantGroupIDForWorkflow(ctx context.Context, workflowID string) (string, error) {
	var groupID string
	err := r.pool.QueryRow(ctx,
		`SELECT t.tenant_group_id
		 FROM workflows ri
		 JOIN tenants t ON t.id = ri.tenant_id
		 WHERE ri.id = $1`,
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
