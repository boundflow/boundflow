package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/convergeplane/convergeplane/internal/domain"
)

type SchedulerRepo struct {
	pool *pgxpool.Pool
}

func NewSchedulerRepo(pool *pgxpool.Pool) *SchedulerRepo {
	return &SchedulerRepo{pool: pool}
}

// GetDuePeriodicResources returns the resource instances in the partition whose next periodic
// invocation is due: repeat_every_seconds has elapsed since the last completion (or since
// creation if it has never run), and the workflow is active. Full instances are returned so the
// caller can build the request's runtime params from the workflow config without a second fetch.
func (r *SchedulerRepo) GetDuePeriodicResources(ctx context.Context, partitionID string) ([]*domain.ResourceInstance, error) {
	rows, err := r.pool.Query(ctx,
		`SELECT id, tenant_id, resource_type,
		        invoke_timeout_seconds, repeat_every_seconds, triggerable,
		        lifecycle_state, workflow_state, lifecycle_policy, invocation_metrics, cooldown_until,
		        lifecycle_last_resolved, current_workflow_version, scheduler_partition_id,
		        target_version, current_version, last_completed_request_at, created_at
		 FROM resource_instances
		 WHERE scheduler_partition_id = $1
		   AND repeat_every_seconds > 0
		   AND workflow_state = 'active'
		   AND lifecycle_state = 'active'
		   AND COALESCE(last_completed_request_at, created_at) + make_interval(secs => repeat_every_seconds) < now()`,
		partitionID,
	)
	if err != nil {
		return nil, fmt.Errorf("get due periodic resources: %w", err)
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
// The write is additionally guarded on the resource's current_version still equaling
// expectedCurrentVersion — the run the caller validated against — so a stale validation
// (a newer run completed in between) results in written=false rather than scheduling.
// Returns written=false (no error) if any guard fails.
// contextJSON and currentAtomicOperation are assembled by the scheduler layer before calling.
func (r *SchedulerRepo) UpsertJobAndSchedule(ctx context.Context, requestID string, contextJSON string, currentAtomicOperation string, timeoutSeconds int, workflowVersion int, expectedCurrentVersion int64) (resourceInstanceID string, version int64, written bool, err error) {
	err = r.pool.QueryRow(ctx,
		`WITH candidate AS (
		     SELECT cr.id, cr.resource_instance_id, cr.version, cr.request_type,
		            ri.resource_type
		     FROM customer_requests cr
		     JOIN resource_instances ri ON ri.id = cr.resource_instance_id
		     WHERE cr.id = $1
		       AND ri.current_version = $6
		 ),
		 upserted AS (
		     INSERT INTO jobs (resource_instance_id, request_id, version, current_atomic_operation, context, status, job_type, resource_type, timeout_seconds, workflow_version)
		     SELECT c.resource_instance_id, c.id, c.version, $2,
		            $3::jsonb,
		            'pending', c.request_type, c.resource_type, $4, $5
		     FROM candidate c
		     ON CONFLICT (resource_instance_id) DO UPDATE
		         SET request_id               = EXCLUDED.request_id,
		             version                  = EXCLUDED.version,
		             current_atomic_operation = EXCLUDED.current_atomic_operation,
		             context                  = EXCLUDED.context,
		             job_type                 = EXCLUDED.job_type,
		             resource_type            = EXCLUDED.resource_type,
		             timeout_seconds          = EXCLUDED.timeout_seconds,
		             workflow_version         = EXCLUDED.workflow_version,
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
		requestID, currentAtomicOperation, contextJSON, timeoutSeconds, workflowVersion, expectedCurrentVersion,
	).Scan(&resourceInstanceID, &version)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", 0, false, nil
		}
		return "", 0, false, fmt.Errorf("upsert job: %w", err)
	}
	return resourceInstanceID, version, true, nil
}

func (r *SchedulerRepo) GetCompletedJobRequestIDs(ctx context.Context, partitionID string) ([]string, error) {
	rows, err := r.pool.Query(ctx,
		`SELECT j.request_id
		 FROM jobs j
		 JOIN resource_instances ri ON j.resource_instance_id = ri.id
		 WHERE ri.scheduler_partition_id = $1
		   AND j.status = 'completed'`,
		partitionID,
	)
	if err != nil {
		return nil, fmt.Errorf("get completed job request ids: %w", err)
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

func (r *SchedulerRepo) GetFailedJobRequestIDs(ctx context.Context, partitionID string) ([]string, error) {
	rows, err := r.pool.Query(ctx,
		`SELECT j.request_id
		 FROM jobs j
		 JOIN resource_instances ri ON j.resource_instance_id = ri.id
		 WHERE ri.scheduler_partition_id = $1
		   AND j.status = 'failed'`,
		partitionID,
	)
	if err != nil {
		return nil, fmt.Errorf("get failed job request ids: %w", err)
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

func (r *SchedulerRepo) DeleteTerminalJob(ctx context.Context, resourceInstanceID string, requestID string) (bool, error) {
	tag, err := r.pool.Exec(ctx,
		`DELETE FROM jobs
		 WHERE resource_instance_id = $1
		   AND request_id = $2
		   AND status IN ('completed', 'failed')`,
		resourceInstanceID, requestID,
	)
	if err != nil {
		return false, fmt.Errorf("delete terminal job: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

func (r *SchedulerRepo) MarkResourceAwaitingApproval(ctx context.Context, resourceInstanceID string) error {
	_, err := r.pool.Exec(ctx,
		`UPDATE resource_instances ri
		 SET lifecycle_state = 'awaiting_approval'
		 FROM jobs j
		 WHERE j.resource_instance_id = ri.id
		   AND j.status = 'awaiting_approval'
		   AND ri.id = $1`,
		resourceInstanceID,
	)
	if err != nil {
		return fmt.Errorf("mark resource awaiting approval: %w", err)
	}
	return nil
}

func (r *SchedulerRepo) SyncAwaitingApprovalStates(ctx context.Context, partitionID string) ([]string, error) {
	rows, err := r.pool.Query(ctx,
		`UPDATE resource_instances ri
		 SET lifecycle_state = 'awaiting_approval'
		 FROM jobs j
		 WHERE j.resource_instance_id = ri.id
		   AND j.status = 'awaiting_approval'
		   AND ri.scheduler_partition_id = $1
		   AND ri.lifecycle_state != 'awaiting_approval'
		 RETURNING ri.id`,
		partitionID,
	)
	if err != nil {
		return nil, fmt.Errorf("sync awaiting approval states: %w", err)
	}
	defer rows.Close()

	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan synced id: %w", err)
		}
		ids = append(ids, id)
	}
	return ids, nil
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
