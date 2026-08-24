package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	boundflowv1 "github.com/boundflow/boundflow/gen/boundflow/v1"
	"github.com/boundflow/boundflow/internal/domain"
)

type JobRepo struct {
	pool *pgxpool.Pool
}

func NewJobRepo(pool *pgxpool.Pool) *JobRepo {
	return &JobRepo{pool: pool}
}

func (r *JobRepo) GetAvailableJob(ctx context.Context, tenantGroupID string, workflowTypes []string, workflowVersions []int32) (*string, error) {
	var workflowID string
	err := r.pool.QueryRow(ctx,
		`SELECT workflow_id FROM jobs
		 WHERE status IN ('pending', 'awaiting_next', 'approved', 'rejected', 'answered', 'input_timed_out')
		   AND (owner IS NULL OR lease_expires_at < now())
		   AND (dispatch_at IS NULL OR dispatch_at <= now())
		   AND tenant_group_id = $1
		   AND (workflow_type, workflow_version) IN (
		       SELECT rt, wv FROM unnest($2::text[], $3::int[]) AS cap(rt, wv)
		   )
		 LIMIT 1`,
		tenantGroupID, workflowTypes, workflowVersions,
	).Scan(&workflowID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("get available job: %w", err)
	}
	return &workflowID, nil
}

func (r *JobRepo) AcquireJob(ctx context.Context, workflowID string, ownerID string, leaseDuration time.Duration, tenantGroupID string) (*domain.Job, error) {
	var job domain.Job
	var contextJSON, agentMetricsJSON, jobMetadataJSON, workflowMetricsJSON, inputAnswerJSON []byte

	err := r.pool.QueryRow(ctx,
		`UPDATE jobs
		 SET owner = $2, lease_expires_at = now() + $3::interval
		 WHERE workflow_id = $1
		   AND status IN ('pending', 'awaiting_next', 'approved', 'rejected', 'answered', 'input_timed_out')
		   AND (owner IS NULL OR lease_expires_at < now())
		   AND (dispatch_at IS NULL OR dispatch_at <= now())
		   AND tenant_group_id = $4
		 RETURNING workflow_id, request_id, version, current_atomic_operation, context, status,
		           job_type, workflow_type, timeout_seconds, workflow_version, agent_metrics, workflow_metrics,
		           job_metadata, approval_id, approval_timeout_at, approval_reason,
		           input_id, input_timeout_at, input_answer,
		           owner, lease_expires_at, abandon_requested_at, created_at`,
		workflowID, ownerID, leaseDuration.String(), tenantGroupID,
	).Scan(
		&job.WorkflowID, &job.RequestID, &job.Version,
		&job.CurrentAtomicOperation, &contextJSON, &job.Status,
		&job.JobType, &job.WorkflowType, &job.RuntimeParams.OperationTimeoutSeconds, &job.WorkflowVersion, &agentMetricsJSON, &workflowMetricsJSON,
		&jobMetadataJSON, &job.ApprovalID, &job.ApprovalTimeoutAt, &job.ApprovalReason,
		&job.InputID, &job.InputTimeoutAt, &inputAnswerJSON,
		&job.Owner, &job.LeaseExpiresAt, &job.AbandonRequestedAt, &job.CreatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("acquire job: %w", err)
	}

	if err := json.Unmarshal(contextJSON, &job.Context); err != nil {
		return nil, fmt.Errorf("unmarshal job context: %w", err)
	}
	if err := json.Unmarshal(agentMetricsJSON, &job.AgentMetrics); err != nil {
		return nil, fmt.Errorf("unmarshal agent metrics: %w", err)
	}
	if err := json.Unmarshal(workflowMetricsJSON, &job.WorkflowMetrics); err != nil {
		return nil, fmt.Errorf("unmarshal workflow metrics: %w", err)
	}
	if err := json.Unmarshal(jobMetadataJSON, &job.JobMetadata); err != nil {
		return nil, fmt.Errorf("unmarshal job metadata: %w", err)
	}
	if inputAnswerJSON != nil {
		if err := json.Unmarshal(inputAnswerJSON, &job.InputAnswer); err != nil {
			return nil, fmt.Errorf("unmarshal input answer: %w", err)
		}
	}

	return &job, nil
}

// RenewJobLease extends the lease and reports whether a suspension has asked for this run
// to be stopped. The renewal tick is the only place already touching the row often enough
// to notice, so the flag rides along rather than costing a second query.
func (r *JobRepo) RenewJobLease(ctx context.Context, workflowID string, ownerID string, leaseDuration time.Duration) (renewed bool, abandonRequested bool, err error) {
	err = r.pool.QueryRow(ctx,
		`UPDATE jobs
		 SET lease_expires_at = now() + $3::interval
		 WHERE workflow_id = $1 AND owner = $2
		 RETURNING abandon_requested_at IS NOT NULL`,
		workflowID, ownerID, leaseDuration.String(),
	).Scan(&abandonRequested)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, false, nil
		}
		return false, false, fmt.Errorf("renew job lease: %w", err)
	}
	return true, abandonRequested, nil
}

func (r *JobRepo) UpdateJobStatus(ctx context.Context, workflowID string, ownerID string, status domain.JobStatus) (bool, error) {
	tag, err := r.pool.Exec(ctx,
		`UPDATE jobs SET status = $3
		 WHERE workflow_id = $1 AND owner = $2`,
		workflowID, ownerID, status,
	)
	if err != nil {
		return false, fmt.Errorf("update job status: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

func (r *JobRepo) UpdateJobStatusWithReason(ctx context.Context, workflowID string, ownerID string, status domain.JobStatus, failureReason string) (bool, error) {
	tag, err := r.pool.Exec(ctx,
		`UPDATE jobs SET status = $3, failure_reason = $4
		 WHERE workflow_id = $1 AND owner = $2`,
		workflowID, ownerID, status, failureReason,
	)
	if err != nil {
		return false, fmt.Errorf("update job status with reason: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

func (r *JobRepo) UpdateJobStatusWithMetrics(ctx context.Context, workflowID string, ownerID string, status domain.JobStatus, resultType domain.RunOutcome, failureReason string, result map[string]any, agentMetrics map[string]*boundflowv1.AgentInvocationMetrics, workflowMetrics domain.WorkflowJobMetrics) (bool, error) {
	agentMetricsJSON, err := json.Marshal(agentMetrics)
	if err != nil {
		return false, fmt.Errorf("marshal agent metrics: %w", err)
	}
	workflowMetricsJSON, err := json.Marshal(workflowMetrics)
	if err != nil {
		return false, fmt.Errorf("marshal workflow metrics: %w", err)
	}
	var resultParam any
	if result != nil {
		resultJSON, err := json.Marshal(result)
		if err != nil {
			return false, fmt.Errorf("marshal result: %w", err)
		}
		resultParam = resultJSON
	}
	tag, err := r.pool.Exec(ctx,
		`UPDATE jobs SET status = $3, result_type = $4, failure_reason = $5, result = $6, agent_metrics = $7, workflow_metrics = $8
		 WHERE workflow_id = $1 AND owner = $2`,
		workflowID, ownerID, status, resultType, failureReason, resultParam, agentMetricsJSON, workflowMetricsJSON,
	)
	if err != nil {
		return false, fmt.Errorf("update job status with metrics: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

func (r *JobRepo) GetJobMetrics(ctx context.Context, workflowID string, requestID string) (map[string]*boundflowv1.AgentInvocationMetrics, domain.WorkflowJobMetrics, error) {
	var agentMetricsJSON, workflowMetricsJSON []byte
	err := r.pool.QueryRow(ctx,
		`SELECT agent_metrics, workflow_metrics FROM jobs WHERE workflow_id = $1 AND request_id = $2`,
		workflowID, requestID,
	).Scan(&agentMetricsJSON, &workflowMetricsJSON)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, domain.WorkflowJobMetrics{}, nil
		}
		return nil, domain.WorkflowJobMetrics{}, fmt.Errorf("get job metrics: %w", err)
	}

	var agentMetrics map[string]*boundflowv1.AgentInvocationMetrics
	if err := json.Unmarshal(agentMetricsJSON, &agentMetrics); err != nil {
		return nil, domain.WorkflowJobMetrics{}, fmt.Errorf("unmarshal agent metrics: %w", err)
	}
	var workflowMetrics domain.WorkflowJobMetrics
	if err := json.Unmarshal(workflowMetricsJSON, &workflowMetrics); err != nil {
		return nil, domain.WorkflowJobMetrics{}, fmt.Errorf("unmarshal workflow metrics: %w", err)
	}
	return agentMetrics, workflowMetrics, nil
}

func (r *JobRepo) ResolveApproval(ctx context.Context, workflowID string, approvalID string, status domain.JobStatus, reason string) (bool, domain.ResolvedApproval, error) {
	var info domain.ResolvedApproval
	err := r.pool.QueryRow(ctx,
		`WITH job_update AS (
		     UPDATE jobs
		     SET status = $3, approval_reason = $4
		     WHERE workflow_id = $1
		       AND approval_id = $2
		       AND status = 'awaiting_approval'
		     RETURNING workflow_id, request_id, tenant_group_id, approval_opened_at,
		               approval_justification
		 ),
		 wf AS (
		     UPDATE workflows
		     SET lifecycle_state = 'invoking'
		     WHERE id IN (SELECT workflow_id FROM job_update)
		 )
		 SELECT request_id, tenant_group_id, approval_opened_at, approval_justification
		 FROM job_update`,
		workflowID, approvalID, status, reason,
	).Scan(&info.RequestID, &info.TenantGroupID, &info.OpenedAt, &info.Justification)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, domain.ResolvedApproval{}, nil
		}
		return false, domain.ResolvedApproval{}, fmt.Errorf("resolve approval: %w", err)
	}
	return true, info, nil
}

// SweepExpiredApprovals atomically rejects the partition's approval gates past their
// timeout and re-queues the workflows (lifecycle_state='invoking', so the rpcworker
// dispatches the on_reject branch). Partition-scoped like ExpireCooldowns: the
// partition owner is unique, so no cross-scheduler locking is needed. Returns the
// resolved gates so the caller can write timed_out audit rows.
func (r *JobRepo) SweepExpiredApprovals(ctx context.Context, partitionID string) ([]domain.ExpiredApproval, error) {
	rows, err := r.pool.Query(ctx,
		`WITH expired AS (
		     UPDATE jobs
		     SET status = 'rejected'
		     WHERE workflow_id IN (SELECT id FROM workflows WHERE scheduler_partition_id = $1)
		       AND status = 'awaiting_approval'
		       AND approval_timeout_at <= now()
		     RETURNING workflow_id, request_id, tenant_group_id, approval_id,
		               approval_timeout_at, approval_opened_at, approval_justification
		 ),
		 wf AS (
		     UPDATE workflows
		     SET lifecycle_state = 'invoking'
		     WHERE id IN (SELECT workflow_id FROM expired)
		 )
		 SELECT workflow_id, request_id, tenant_group_id, approval_id, approval_timeout_at,
		        approval_opened_at, approval_justification
		 FROM expired`,
		partitionID,
	)
	if err != nil {
		return nil, fmt.Errorf("sweep expired approvals: %w", err)
	}
	defer rows.Close()

	var out []domain.ExpiredApproval
	for rows.Next() {
		var e domain.ExpiredApproval
		if err := rows.Scan(&e.WorkflowID, &e.RequestID, &e.TenantGroupID, &e.ApprovalID, &e.TimedOutAt, &e.OpenedAt, &e.Justification); err != nil {
			return nil, fmt.Errorf("scan expired approval: %w", err)
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

func (r *JobRepo) ParkForApproval(ctx context.Context, workflowID string, ownerID string, approvalID string, timeoutSeconds int, justification string, approvalMetadata map[string]any, metadata domain.JobMetadata, agentMetrics map[string]*boundflowv1.AgentInvocationMetrics, workflowMetrics domain.WorkflowJobMetrics) (bool, error) {
	metadataJSON, err := json.Marshal(metadata)
	if err != nil {
		return false, fmt.Errorf("marshal job metadata: %w", err)
	}
	agentMetricsJSON, err := json.Marshal(agentMetrics)
	if err != nil {
		return false, fmt.Errorf("marshal agent metrics: %w", err)
	}
	workflowMetricsJSON, err := json.Marshal(workflowMetrics)
	if err != nil {
		return false, fmt.Errorf("marshal workflow metrics: %w", err)
	}
	var approvalMetadataParam any
	if approvalMetadata != nil {
		approvalMetadataJSON, err := json.Marshal(approvalMetadata)
		if err != nil {
			return false, fmt.Errorf("marshal approval metadata: %w", err)
		}
		approvalMetadataParam = approvalMetadataJSON
	}
	tag, err := r.pool.Exec(ctx,
		`UPDATE jobs
		 SET status = $3, approval_id = $4,
		     approval_opened_at = now(), approval_timeout_at = now() + make_interval(secs => $5),
		     approval_justification = $6, approval_metadata = $7,
		     job_metadata = $8, agent_metrics = $9, workflow_metrics = $10,
		     context = '{}'::jsonb, timeout_seconds = 0, current_atomic_operation = ''
		 WHERE workflow_id = $1 AND owner = $2`,
		workflowID, ownerID, domain.JobStatusAwaitingApproval, approvalID, timeoutSeconds,
		justification, approvalMetadataParam, metadataJSON, agentMetricsJSON, workflowMetricsJSON,
	)
	if err != nil {
		return false, fmt.Errorf("park job for approval: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

func (r *JobRepo) ResolveInput(ctx context.Context, workflowID string, inputID string, answer map[string]any) (bool, domain.ResolvedInput, error) {
	var info domain.ResolvedInput
	var answerParam any
	if answer != nil {
		answerJSON, err := json.Marshal(answer)
		if err != nil {
			return false, domain.ResolvedInput{}, fmt.Errorf("marshal answer: %w", err)
		}
		answerParam = answerJSON
	}
	err := r.pool.QueryRow(ctx,
		`WITH job_update AS (
		     UPDATE jobs
		     SET status = $3, input_answer = $4
		     WHERE workflow_id = $1
		       AND input_id = $2
		       AND status = 'awaiting_input'
		     RETURNING workflow_id, request_id, tenant_group_id, input_opened_at, input_prompt
		 ),
		 wf AS (
		     UPDATE workflows
		     SET lifecycle_state = 'invoking'
		     WHERE id IN (SELECT workflow_id FROM job_update)
		 )
		 SELECT request_id, tenant_group_id, input_opened_at, input_prompt FROM job_update`,
		workflowID, inputID, domain.JobStatusAnswered, answerParam,
	).Scan(&info.RequestID, &info.TenantGroupID, &info.OpenedAt, &info.Prompt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, domain.ResolvedInput{}, nil
		}
		return false, domain.ResolvedInput{}, fmt.Errorf("resolve input: %w", err)
	}
	return true, info, nil
}

// SweepExpiredInputs atomically times out the partition's input gates past their
// deadline and re-queues the workflows (lifecycle_state='invoking', so the
// rpcworker dispatches the on_timeout branch). Partition-scoped like
// SweepExpiredApprovals: the partition owner is unique, so no cross-scheduler
// locking is needed. Returns the resolved gates so the caller can write timed_out
// audit rows.
func (r *JobRepo) SweepExpiredInputs(ctx context.Context, partitionID string) ([]domain.ExpiredInput, error) {
	rows, err := r.pool.Query(ctx,
		`WITH expired AS (
		     UPDATE jobs
		     SET status = 'input_timed_out'
		     WHERE workflow_id IN (SELECT id FROM workflows WHERE scheduler_partition_id = $1)
		       AND status = 'awaiting_input'
		       AND input_timeout_at <= now()
		     RETURNING workflow_id, request_id, tenant_group_id, input_id,
		               input_timeout_at, input_opened_at, input_prompt
		 ),
		 wf AS (
		     UPDATE workflows
		     SET lifecycle_state = 'invoking'
		     WHERE id IN (SELECT workflow_id FROM expired)
		 )
		 SELECT workflow_id, request_id, tenant_group_id, input_id, input_timeout_at,
		        input_opened_at, input_prompt
		 FROM expired`,
		partitionID,
	)
	if err != nil {
		return nil, fmt.Errorf("sweep expired inputs: %w", err)
	}
	defer rows.Close()

	var out []domain.ExpiredInput
	for rows.Next() {
		var e domain.ExpiredInput
		if err := rows.Scan(&e.WorkflowID, &e.RequestID, &e.TenantGroupID, &e.InputID, &e.TimedOutAt, &e.OpenedAt, &e.Prompt); err != nil {
			return nil, fmt.Errorf("scan expired input: %w", err)
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

func (r *JobRepo) ParkForInput(ctx context.Context, workflowID string, ownerID string, inputID string, timeoutSeconds int, prompt string, inputMetadata map[string]any, metadata domain.JobMetadata, agentMetrics map[string]*boundflowv1.AgentInvocationMetrics, workflowMetrics domain.WorkflowJobMetrics) (bool, error) {
	metadataJSON, err := json.Marshal(metadata)
	if err != nil {
		return false, fmt.Errorf("marshal job metadata: %w", err)
	}
	agentMetricsJSON, err := json.Marshal(agentMetrics)
	if err != nil {
		return false, fmt.Errorf("marshal agent metrics: %w", err)
	}
	workflowMetricsJSON, err := json.Marshal(workflowMetrics)
	if err != nil {
		return false, fmt.Errorf("marshal workflow metrics: %w", err)
	}
	var inputMetadataParam any
	if inputMetadata != nil {
		inputMetadataJSON, err := json.Marshal(inputMetadata)
		if err != nil {
			return false, fmt.Errorf("marshal input metadata: %w", err)
		}
		inputMetadataParam = inputMetadataJSON
	}
	tag, err := r.pool.Exec(ctx,
		`UPDATE jobs
		 SET status = $3, input_id = $4,
		     input_opened_at = now(), input_timeout_at = now() + make_interval(secs => $5),
		     input_prompt = $6, input_metadata = $7,
		     job_metadata = $8, agent_metrics = $9, workflow_metrics = $10,
		     context = '{}'::jsonb, timeout_seconds = 0, current_atomic_operation = ''
		 WHERE workflow_id = $1 AND owner = $2`,
		workflowID, ownerID, domain.JobStatusAwaitingInput, inputID, timeoutSeconds,
		prompt, inputMetadataParam, metadataJSON, agentMetricsJSON, workflowMetricsJSON,
	)
	if err != nil {
		return false, fmt.Errorf("park job for input: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

// MarkAbandonRequested tells whoever holds the job to stop the run, for a suspension
// the operator asked to stop rather than drain. Reports whether a job was marked.
//
// Not ownership-guarded: the point is to reach the job whether or not a worker is on
// it. A live one picks the flag up when it renews its lease; an unclaimed one is
// completed at dispatch without ever launching, so nothing is left holding the
// suspension open.
//
// Must be called after MarkSuspensionRequested has returned, never folded into it: the
// freeze there is the barrier that makes every racing job insert visible, and only a
// statement whose snapshot is taken afterwards can see them.
//
// Guarded on suspensionID rather than merely on the workflow being suspended, so a stale
// retry from an earlier suspension cannot cut a run belonging to a later one that only
// asked to drain. Terminal jobs are excluded — they are about to be deleted — and the
// write is guarded on the flag being unset so the reconciler's re-runs keep the original
// timestamp rather than pushing it forward.
func (r *JobRepo) MarkAbandonRequested(ctx context.Context, workflowID, suspensionID string) (bool, error) {
	tag, err := r.pool.Exec(ctx,
		`UPDATE jobs SET abandon_requested_at = now()
		 WHERE workflow_id = $1
		   AND abandon_requested_at IS NULL
		   AND status NOT IN ('completed', 'failed')
		   AND EXISTS (
		       SELECT 1 FROM workflows w
		       WHERE w.id = $1 AND w.suspension_id = $2
		   )`,
		workflowID, suspensionID,
	)
	if err != nil {
		return false, fmt.Errorf("mark abandon requested: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

func (r *JobRepo) MarkOrphanedJobsFailed(ctx context.Context, partitionID string, gracePeriodSeconds int) (int, error) {
	tag, err := r.pool.Exec(ctx,
		`UPDATE jobs
		 SET status = 'failed'
		 WHERE workflow_id IN (SELECT id FROM workflows WHERE scheduler_partition_id = $1)
		   AND status IN ('dispatched', 'running')
		   AND (owner IS NULL
		        OR lease_expires_at < now() - make_interval(secs => $2))`,
		partitionID, gracePeriodSeconds,
	)
	if err != nil {
		return 0, fmt.Errorf("mark orphaned jobs failed: %w", err)
	}
	return int(tag.RowsAffected()), nil
}

func (r *JobRepo) SetJobDispatched(ctx context.Context, workflowID string, ownerID string) (bool, error) {
	tag, err := r.pool.Exec(ctx,
		`UPDATE jobs
		 SET status = 'dispatched'
		 WHERE workflow_id = $1 AND owner = $2 AND status != 'dispatched'`,
		workflowID, ownerID,
	)
	if err != nil {
		return false, fmt.Errorf("set job dispatched: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

func (r *JobRepo) ReleaseJob(ctx context.Context, workflowID string, ownerID string) error {
	_, err := r.pool.Exec(ctx,
		`UPDATE jobs
		 SET owner = NULL, lease_expires_at = NULL
		 WHERE workflow_id = $1 AND owner = $2`,
		workflowID, ownerID,
	)
	if err != nil {
		return fmt.Errorf("release job: %w", err)
	}
	return nil
}

func (r *JobRepo) UpdateJob(ctx context.Context, workflowID string, ownerID string, status domain.JobStatus, currentAtomicOperation string, operationTimeoutSeconds int, jobContext map[string]any) (bool, error) {
	contextJSON, err := json.Marshal(jobContext)
	if err != nil {
		return false, fmt.Errorf("marshal job context: %w", err)
	}

	tag, err := r.pool.Exec(ctx,
		`UPDATE jobs
		 SET status = $3, current_atomic_operation = $4, timeout_seconds = $5, context = $6,
		     approval_id = NULL, approval_timeout_at = NULL, approval_justification = '', approval_metadata = NULL, approval_reason = '',
		     input_id = NULL, input_timeout_at = NULL, input_prompt = '', input_metadata = NULL, input_answer = NULL,
		     job_metadata = '{}'
		 WHERE workflow_id = $1 AND owner = $2`,
		workflowID, ownerID, status, currentAtomicOperation, operationTimeoutSeconds, contextJSON,
	)
	if err != nil {
		return false, fmt.Errorf("update job: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

func (r *JobRepo) UpdateJobWithMetrics(ctx context.Context, workflowID string, ownerID string, status domain.JobStatus, currentAtomicOperation string, operationTimeoutSeconds int, delaySeconds int, jobContext map[string]any, agentMetrics map[string]*boundflowv1.AgentInvocationMetrics, workflowMetrics domain.WorkflowJobMetrics) (bool, error) {
	contextJSON, err := json.Marshal(jobContext)
	if err != nil {
		return false, fmt.Errorf("marshal job context: %w", err)
	}
	agentMetricsJSON, err := json.Marshal(agentMetrics)
	if err != nil {
		return false, fmt.Errorf("marshal agent metrics: %w", err)
	}
	workflowMetricsJSON, err := json.Marshal(workflowMetrics)
	if err != nil {
		return false, fmt.Errorf("marshal workflow metrics: %w", err)
	}

	tag, err := r.pool.Exec(ctx,
		`UPDATE jobs
		 SET status = $3, current_atomic_operation = $4, timeout_seconds = $5, context = $6, agent_metrics = $7, workflow_metrics = $8,
		     dispatch_at = CASE WHEN $9::int > 0 THEN now() + make_interval(secs => $9::int) ELSE NULL END,
		     approval_id = NULL, approval_timeout_at = NULL, approval_justification = '', approval_metadata = NULL, approval_reason = '',
		     input_id = NULL, input_timeout_at = NULL, input_prompt = '', input_metadata = NULL, input_answer = NULL,
		     job_metadata = '{}'
		 WHERE workflow_id = $1 AND owner = $2`,
		workflowID, ownerID, status, currentAtomicOperation, operationTimeoutSeconds, contextJSON, agentMetricsJSON, workflowMetricsJSON, delaySeconds,
	)
	if err != nil {
		return false, fmt.Errorf("update job with metrics: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}
