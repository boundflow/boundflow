package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type SchedulerRepo struct {
	pool *pgxpool.Pool
}

func NewSchedulerRepo(pool *pgxpool.Pool) *SchedulerRepo {
	return &SchedulerRepo{pool: pool}
}

func (r *SchedulerRepo) GetTopUnscheduledRequests(ctx context.Context, partitionID string) ([]string, error) {
	rows, err := r.pool.Query(ctx,
		`SELECT DISTINCT ON (cr.resource_instance_id) cr.id
		 FROM customer_requests cr
		 JOIN resource_instances ri ON cr.resource_instance_id = ri.id
		 WHERE ri.scheduler_partition_id = $1
		   AND cr.status = 'unscheduled'
		 ORDER BY cr.resource_instance_id, cr.version DESC`,
		partitionID,
	)
	if err != nil {
		return nil, fmt.Errorf("get top unscheduled requests: %w", err)
	}
	defer rows.Close()

	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan request id: %w", err)
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

// UpsertJobAndSchedule writes or overwrites the job for the resource associated with requestID,
// but only if the request's version is strictly higher than the job currently in the table
// (and that job is still pending). Atomically marks the request as scheduled if written.
// Returns written=false (no error) if the existing job had an equal or higher version or the job has been started already.
func (r *SchedulerRepo) UpsertJobAndSchedule(ctx context.Context, requestID string) (resourceInstanceID string, version int64, written bool, err error) {
	err = r.pool.QueryRow(ctx,
		`WITH candidate AS (
		     SELECT cr.id, cr.resource_instance_id, cr.version, cr.request_type, cr.request_info
		     FROM customer_requests cr
		     WHERE cr.id = $1
		 ),
		 upserted AS (
		     INSERT INTO jobs (resource_instance_id, request_id, version, current_atomic_operation, context, status, job_type)
		     SELECT c.resource_instance_id, c.id, c.version, '', c.request_info, 'pending', c.request_type
		     FROM candidate c
		     ON CONFLICT (resource_instance_id) DO UPDATE
		         SET request_id               = EXCLUDED.request_id,
		             version                  = EXCLUDED.version,
		             current_atomic_operation = '',
		             context                  = EXCLUDED.context,
		             job_type                 = EXCLUDED.job_type,
		             status                   = 'pending',
		             owner                    = NULL,
		             lease_expires_at         = NULL
		         WHERE jobs.version < EXCLUDED.version
		           AND jobs.status = 'pending'
		     RETURNING resource_instance_id, request_id
		 )
		 UPDATE customer_requests cr
		 SET status = 'scheduled'
		 FROM upserted u
		 WHERE cr.id = u.request_id
		 RETURNING cr.resource_instance_id, cr.version`,
		requestID,
	).Scan(&resourceInstanceID, &version)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", 0, false, nil
		}
		return "", 0, false, fmt.Errorf("upsert job: %w", err)
	}
	return resourceInstanceID, version, true, nil
}

func (r *SchedulerRepo) SupercedeOlderRequests(ctx context.Context, resourceInstanceID string, version int64) error {
	_, err := r.pool.Exec(ctx,
		`UPDATE customer_requests
		 SET status = 'superceded'
		 WHERE resource_instance_id = $1
		   AND status IN ('unscheduled', 'scheduled')
		   AND version < $2`,
		resourceInstanceID, version,
	)
	if err != nil {
		return fmt.Errorf("supercede older requests: %w", err)
	}
	return nil
}
