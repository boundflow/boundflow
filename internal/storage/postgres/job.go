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
		 WHERE status IN ('pending', 'awaiting_next', 'approved', 'rejected')
		   AND (owner IS NULL OR lease_expires_at < now())
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
	var contextJSON, agentMetricsJSON, jobMetadataJSON, workflowMetricsJSON []byte

	err := r.pool.QueryRow(ctx,
		`UPDATE jobs
		 SET owner = $2, lease_expires_at = now() + $3::interval
		 WHERE workflow_id = $1
		   AND status IN ('pending', 'awaiting_next', 'approved', 'rejected')
		   AND (owner IS NULL OR lease_expires_at < now())
		   AND tenant_group_id = $4
		 RETURNING workflow_id, request_id, version, current_atomic_operation, context, status,
		           job_type, workflow_type, timeout_seconds, workflow_version, agent_metrics, workflow_metrics,
		           job_metadata, approval_id, approval_timeout_at,
		           owner, lease_expires_at, created_at`,
		workflowID, ownerID, leaseDuration.String(), tenantGroupID,
	).Scan(
		&job.WorkflowID, &job.RequestID, &job.Version,
		&job.CurrentAtomicOperation, &contextJSON, &job.Status,
		&job.JobType, &job.WorkflowType, &job.RuntimeParams.OperationTimeoutSeconds, &job.WorkflowVersion, &agentMetricsJSON, &workflowMetricsJSON,
		&jobMetadataJSON, &job.ApprovalID, &job.ApprovalTimeoutAt,
		&job.Owner, &job.LeaseExpiresAt, &job.CreatedAt,
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

	return &job, nil
}

func (r *JobRepo) RenewJobLease(ctx context.Context, workflowID string, ownerID string, leaseDuration time.Duration) (bool, error) {
	tag, err := r.pool.Exec(ctx,
		`UPDATE jobs
		 SET lease_expires_at = now() + $3::interval
		 WHERE workflow_id = $1 AND owner = $2`,
		workflowID, ownerID, leaseDuration.String(),
	)
	if err != nil {
		return false, fmt.Errorf("renew job lease: %w", err)
	}
	return tag.RowsAffected() == 1, nil
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

func (r *JobRepo) UpdateJobStatusWithMetrics(ctx context.Context, workflowID string, ownerID string, status domain.JobStatus, agentMetrics map[string]*boundflowv1.AgentInvocationMetrics, workflowMetrics domain.WorkflowJobMetrics) (bool, error) {
	agentMetricsJSON, err := json.Marshal(agentMetrics)
	if err != nil {
		return false, fmt.Errorf("marshal agent metrics: %w", err)
	}
	workflowMetricsJSON, err := json.Marshal(workflowMetrics)
	if err != nil {
		return false, fmt.Errorf("marshal workflow metrics: %w", err)
	}
	tag, err := r.pool.Exec(ctx,
		`UPDATE jobs SET status = $3, agent_metrics = $4, workflow_metrics = $5
		 WHERE workflow_id = $1 AND owner = $2`,
		workflowID, ownerID, status, agentMetricsJSON, workflowMetricsJSON,
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

func (r *JobRepo) ResolveApproval(ctx context.Context, workflowID string, approvalID string, status domain.JobStatus) (bool, domain.ResolvedApproval, error) {
	var info domain.ResolvedApproval
	err := r.pool.QueryRow(ctx,
		`WITH job_update AS (
		     UPDATE jobs
		     SET status = $3
		     WHERE workflow_id = $1
		       AND approval_id = $2
		       AND status = 'awaiting_approval'
		     RETURNING workflow_id, request_id, tenant_group_id, approval_opened_at
		 ),
		 wf AS (
		     UPDATE workflows
		     SET lifecycle_state = 'invoking'
		     WHERE id IN (SELECT workflow_id FROM job_update)
		 )
		 SELECT request_id, tenant_group_id, approval_opened_at FROM job_update`,
		workflowID, approvalID, status,
	).Scan(&info.RequestID, &info.TenantGroupID, &info.OpenedAt)
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
		               approval_timeout_at, approval_opened_at
		 ),
		 wf AS (
		     UPDATE workflows SET lifecycle_state = 'invoking'
		     WHERE id IN (SELECT workflow_id FROM expired)
		 )
		 SELECT workflow_id, request_id, tenant_group_id, approval_id, approval_timeout_at, approval_opened_at FROM expired`,
		partitionID,
	)
	if err != nil {
		return nil, fmt.Errorf("sweep expired approvals: %w", err)
	}
	defer rows.Close()

	var out []domain.ExpiredApproval
	for rows.Next() {
		var e domain.ExpiredApproval
		if err := rows.Scan(&e.WorkflowID, &e.RequestID, &e.TenantGroupID, &e.ApprovalID, &e.TimedOutAt, &e.OpenedAt); err != nil {
			return nil, fmt.Errorf("scan expired approval: %w", err)
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

func (r *JobRepo) ParkForApproval(ctx context.Context, workflowID string, ownerID string, approvalID string, timeoutSeconds int, metadata domain.JobMetadata, agentMetrics map[string]*boundflowv1.AgentInvocationMetrics, workflowMetrics domain.WorkflowJobMetrics) (bool, error) {
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
	tag, err := r.pool.Exec(ctx,
		`UPDATE jobs
		 SET status = $3, approval_id = $4,
		     approval_opened_at = now(), approval_timeout_at = now() + make_interval(secs => $5),
		     job_metadata = $6, agent_metrics = $7, workflow_metrics = $8,
		     context = '{}'::jsonb, timeout_seconds = 0, current_atomic_operation = ''
		 WHERE workflow_id = $1 AND owner = $2`,
		workflowID, ownerID, domain.JobStatusAwaitingApproval, approvalID, timeoutSeconds, metadataJSON, agentMetricsJSON, workflowMetricsJSON,
	)
	if err != nil {
		return false, fmt.Errorf("park job for approval: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

func (r *JobRepo) MarkOrphanedJobsFailed(ctx context.Context, partitionID string, gracePeriodSeconds int) (int, error) {
	tag, err := r.pool.Exec(ctx,
		`UPDATE jobs
		 SET status = 'failed'
		 WHERE workflow_id IN (SELECT id FROM workflows WHERE scheduler_partition_id = $1)
		   AND status IN ('dispatched', 'running')
		   AND lease_expires_at < now() - make_interval(secs => $2)`,
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
		     approval_id = NULL, approval_timeout_at = NULL, job_metadata = '{}'
		 WHERE workflow_id = $1 AND owner = $2`,
		workflowID, ownerID, status, currentAtomicOperation, operationTimeoutSeconds, contextJSON,
	)
	if err != nil {
		return false, fmt.Errorf("update job: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

func (r *JobRepo) UpdateJobWithMetrics(ctx context.Context, workflowID string, ownerID string, status domain.JobStatus, currentAtomicOperation string, operationTimeoutSeconds int, jobContext map[string]any, agentMetrics map[string]*boundflowv1.AgentInvocationMetrics, workflowMetrics domain.WorkflowJobMetrics) (bool, error) {
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
		     approval_id = NULL, approval_timeout_at = NULL, job_metadata = '{}'
		 WHERE workflow_id = $1 AND owner = $2`,
		workflowID, ownerID, status, currentAtomicOperation, operationTimeoutSeconds, contextJSON, agentMetricsJSON, workflowMetricsJSON,
	)
	if err != nil {
		return false, fmt.Errorf("update job with metrics: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}
