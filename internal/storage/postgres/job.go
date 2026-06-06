package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	convergeplanev1 "github.com/convergeplane/convergeplane/gen/convergeplane/v1"
	"github.com/convergeplane/convergeplane/internal/domain"
)

type JobRepo struct {
	pool *pgxpool.Pool
}

func NewJobRepo(pool *pgxpool.Pool) *JobRepo {
	return &JobRepo{pool: pool}
}

func (r *JobRepo) GetAvailableJob(ctx context.Context) (*string, error) {
	var resourceInstanceID string
	err := r.pool.QueryRow(ctx,
		`SELECT resource_instance_id FROM jobs
		 WHERE (
		     status IN ('pending', 'awaiting_next', 'approved', 'rejected')
		     OR (status = 'awaiting_approval' AND approval_timeout_at <= now())
		 )
		   AND (owner IS NULL OR lease_expires_at < now())
		 LIMIT 1`,
	).Scan(&resourceInstanceID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("get available job: %w", err)
	}
	return &resourceInstanceID, nil
}

func (r *JobRepo) AcquireJob(ctx context.Context, resourceInstanceID string, ownerID string, leaseDuration time.Duration) (*domain.Job, error) {
	var job domain.Job
	var contextJSON, agentMetricsJSON, jobMetadataJSON, workflowMetricsJSON []byte

	err := r.pool.QueryRow(ctx,
		`UPDATE jobs
		 SET owner = $2, lease_expires_at = now() + $3::interval
		 WHERE resource_instance_id = $1
		   AND (
		       status IN ('pending', 'awaiting_next', 'approved', 'rejected')
		       OR (status = 'awaiting_approval' AND approval_timeout_at <= now())
		   )
		   AND (owner IS NULL OR lease_expires_at < now())
		 RETURNING resource_instance_id, request_id, version, current_atomic_operation, context, status,
		           job_type, resource_type, timeout_seconds, workflow_version, agent_metrics, workflow_metrics,
		           job_metadata, approval_id, approval_timeout_at,
		           owner, lease_expires_at, created_at`,
		resourceInstanceID, ownerID, leaseDuration.String(),
	).Scan(
		&job.ResourceInstanceID, &job.RequestID, &job.Version,
		&job.CurrentAtomicOperation, &contextJSON, &job.Status,
		&job.JobType, &job.ResourceType, &job.RuntimeParams.OperationTimeoutSeconds, &job.WorkflowVersion, &agentMetricsJSON, &workflowMetricsJSON,
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

func (r *JobRepo) RenewJobLease(ctx context.Context, resourceInstanceID string, ownerID string, leaseDuration time.Duration) (bool, error) {
	tag, err := r.pool.Exec(ctx,
		`UPDATE jobs
		 SET lease_expires_at = now() + $3::interval
		 WHERE resource_instance_id = $1 AND owner = $2`,
		resourceInstanceID, ownerID, leaseDuration.String(),
	)
	if err != nil {
		return false, fmt.Errorf("renew job lease: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

func (r *JobRepo) UpdateJobStatus(ctx context.Context, resourceInstanceID string, ownerID string, status domain.JobStatus) (bool, error) {
	tag, err := r.pool.Exec(ctx,
		`UPDATE jobs SET status = $3
		 WHERE resource_instance_id = $1 AND owner = $2`,
		resourceInstanceID, ownerID, status,
	)
	if err != nil {
		return false, fmt.Errorf("update job status: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

func (r *JobRepo) UpdateJobStatusWithMetrics(ctx context.Context, resourceInstanceID string, ownerID string, status domain.JobStatus, agentMetrics map[string]*convergeplanev1.AgentInvocationMetrics, workflowMetrics domain.WorkflowJobMetrics) (bool, error) {
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
		 WHERE resource_instance_id = $1 AND owner = $2`,
		resourceInstanceID, ownerID, status, agentMetricsJSON, workflowMetricsJSON,
	)
	if err != nil {
		return false, fmt.Errorf("update job status with metrics: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

func (r *JobRepo) GetJobMetrics(ctx context.Context, resourceInstanceID string, requestID string) (map[string]*convergeplanev1.AgentInvocationMetrics, domain.WorkflowJobMetrics, error) {
	var agentMetricsJSON, workflowMetricsJSON []byte
	err := r.pool.QueryRow(ctx,
		`SELECT agent_metrics, workflow_metrics FROM jobs WHERE resource_instance_id = $1 AND request_id = $2`,
		resourceInstanceID, requestID,
	).Scan(&agentMetricsJSON, &workflowMetricsJSON)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, domain.WorkflowJobMetrics{}, nil
		}
		return nil, domain.WorkflowJobMetrics{}, fmt.Errorf("get job metrics: %w", err)
	}

	var agentMetrics map[string]*convergeplanev1.AgentInvocationMetrics
	if err := json.Unmarshal(agentMetricsJSON, &agentMetrics); err != nil {
		return nil, domain.WorkflowJobMetrics{}, fmt.Errorf("unmarshal agent metrics: %w", err)
	}
	var workflowMetrics domain.WorkflowJobMetrics
	if err := json.Unmarshal(workflowMetricsJSON, &workflowMetrics); err != nil {
		return nil, domain.WorkflowJobMetrics{}, fmt.Errorf("unmarshal workflow metrics: %w", err)
	}
	return agentMetrics, workflowMetrics, nil
}

func (r *JobRepo) ResolveApproval(ctx context.Context, resourceInstanceID string, approvalID string, status domain.JobStatus) (bool, error) {
	var updated string
	err := r.pool.QueryRow(ctx,
		`WITH job_update AS (
		     UPDATE jobs
		     SET status = $3
		     WHERE resource_instance_id = $1
		       AND approval_id = $2
		       AND status = 'awaiting_approval'
		     RETURNING resource_instance_id
		 )
		 UPDATE resource_instances
		 SET lifecycle_state = 'reconciling'
		 WHERE id IN (SELECT resource_instance_id FROM job_update)
		 RETURNING id`,
		resourceInstanceID, approvalID, status,
	).Scan(&updated)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, nil
		}
		return false, fmt.Errorf("resolve approval: %w", err)
	}
	return true, nil
}

func (r *JobRepo) ParkForApproval(ctx context.Context, resourceInstanceID string, ownerID string, approvalID string, timeoutAt time.Time, metadata domain.JobMetadata) (bool, error) {
	metadataJSON, err := json.Marshal(metadata)
	if err != nil {
		return false, fmt.Errorf("marshal job metadata: %w", err)
	}
	tag, err := r.pool.Exec(ctx,
		`UPDATE jobs
		 SET status = $3, approval_id = $4, approval_timeout_at = $5, job_metadata = $6,
		     context = '{}'::jsonb, timeout_seconds = 0, current_atomic_operation = ''
		 WHERE resource_instance_id = $1 AND owner = $2`,
		resourceInstanceID, ownerID, domain.JobStatusAwaitingApproval, approvalID, timeoutAt, metadataJSON,
	)
	if err != nil {
		return false, fmt.Errorf("park job for approval: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

func (r *JobRepo) ReleaseJob(ctx context.Context, resourceInstanceID string, ownerID string) error {
	_, err := r.pool.Exec(ctx,
		`UPDATE jobs
		 SET owner = NULL, lease_expires_at = NULL
		 WHERE resource_instance_id = $1 AND owner = $2`,
		resourceInstanceID, ownerID,
	)
	if err != nil {
		return fmt.Errorf("release job: %w", err)
	}
	return nil
}

func (r *JobRepo) UpdateJob(ctx context.Context, resourceInstanceID string, ownerID string, status domain.JobStatus, currentAtomicOperation string, operationTimeoutSeconds int, jobContext map[string]any) (bool, error) {
	contextJSON, err := json.Marshal(jobContext)
	if err != nil {
		return false, fmt.Errorf("marshal job context: %w", err)
	}

	tag, err := r.pool.Exec(ctx,
		`UPDATE jobs
		 SET status = $3, current_atomic_operation = $4, timeout_seconds = $5, context = $6,
		     approval_id = NULL, approval_timeout_at = NULL, job_metadata = '{}'
		 WHERE resource_instance_id = $1 AND owner = $2`,
		resourceInstanceID, ownerID, status, currentAtomicOperation, operationTimeoutSeconds, contextJSON,
	)
	if err != nil {
		return false, fmt.Errorf("update job: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

func (r *JobRepo) UpdateJobWithMetrics(ctx context.Context, resourceInstanceID string, ownerID string, status domain.JobStatus, currentAtomicOperation string, operationTimeoutSeconds int, jobContext map[string]any, agentMetrics map[string]*convergeplanev1.AgentInvocationMetrics, workflowMetrics domain.WorkflowJobMetrics) (bool, error) {
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
		 WHERE resource_instance_id = $1 AND owner = $2`,
		resourceInstanceID, ownerID, status, currentAtomicOperation, operationTimeoutSeconds, contextJSON, agentMetricsJSON, workflowMetricsJSON,
	)
	if err != nil {
		return false, fmt.Errorf("update job with metrics: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}
