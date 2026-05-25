package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

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
		 WHERE status IN ('pending', 'awaiting_next')
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
	var contextJSON []byte

	err := r.pool.QueryRow(ctx,
		`UPDATE jobs
		 SET owner = $2, lease_expires_at = now() + $3::interval
		 WHERE resource_instance_id = $1
		   AND status IN ('pending', 'awaiting_next')
		   AND (owner IS NULL OR lease_expires_at < now())
		 RETURNING resource_instance_id, request_id, version, current_atomic_operation, context, status, job_type, resource_type, timeout_seconds, owner, lease_expires_at, created_at`,
		resourceInstanceID, ownerID, leaseDuration.String(),
	).Scan(
		&job.ResourceInstanceID, &job.RequestID, &job.Version,
		&job.CurrentAtomicOperation, &contextJSON, &job.Status,
		&job.JobType, &job.ResourceType, &job.RuntimeParams.OperationTimeoutSeconds, &job.Owner, &job.LeaseExpiresAt, &job.CreatedAt,
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
		 SET status = $3, current_atomic_operation = $4, timeout_seconds = $5, context = $6
		 WHERE resource_instance_id = $1 AND owner = $2`,
		resourceInstanceID, ownerID, status, currentAtomicOperation, operationTimeoutSeconds, contextJSON,
	)
	if err != nil {
		return false, fmt.Errorf("update job: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}
