package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/boundflow/boundflow/internal/domain"
)

type SchedulerRepo struct {
	pool *pgxpool.Pool
}

func NewSchedulerRepo(pool *pgxpool.Pool) *SchedulerRepo {
	return &SchedulerRepo{pool: pool}
}

// GetDuePeriodicWorkflows returns the workflow instances in the partition whose next periodic
// invocation is due: repeat_every_seconds has elapsed since the last completion (or since
// creation if it has never run), and the workflow is active. Full instances are returned so the
// caller can build the request's runtime params from the workflow config without a second fetch.
func (r *SchedulerRepo) GetDuePeriodicWorkflows(ctx context.Context, partitionID string) ([]*domain.Workflow, error) {
	rows, err := r.pool.Query(ctx,
		`SELECT id, tenant_id, workflow_type,
		        invoke_timeout_seconds, repeat_every_seconds, triggerable,
		        lifecycle_state, workflow_state, lifecycle_policy, invocation_metrics, cooldown_until,
		        lifecycle_last_resolved, current_workflow_version, scheduler_partition_id,
		        target_version, current_version, last_completed_request_at, created_at
		 FROM workflows
		 WHERE scheduler_partition_id = $1
		   AND repeat_every_seconds > 0
		   AND workflow_state = 'active'
		   AND lifecycle_state = 'active'
		   AND COALESCE(last_completed_request_at, created_at) + make_interval(secs => repeat_every_seconds) < now()`,
		partitionID,
	)
	if err != nil {
		return nil, fmt.Errorf("get due periodic workflows: %w", err)
	}
	defer rows.Close()

	var instances []*domain.Workflow
	for rows.Next() {
		var inst domain.Workflow
		var lifecyclePolicyJSON, invocationMetricsJSON []byte
		if err := rows.Scan(
			&inst.ID, &inst.TenantID, &inst.WorkflowType,
			&inst.WorkflowConfig.InvokeTimeoutSeconds,
			&inst.WorkflowConfig.RepeatEverySeconds,
			&inst.WorkflowConfig.Triggerable,
			&inst.LifecycleState, &inst.WorkflowState,
			&lifecyclePolicyJSON, &invocationMetricsJSON, &inst.CooldownUntil,
			&inst.LifecycleLastResolved, &inst.CurrentWorkflowVersion, &inst.SchedulerPartitionID,
			&inst.TargetVersion, &inst.CurrentVersion,
			&inst.LastCompletedRequestAt, &inst.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan workflow instance: %w", err)
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
		`SELECT DISTINCT ON (cr.workflow_id) cr.id
		 FROM customer_requests cr
		 JOIN workflows ri ON cr.workflow_id = ri.id
		 WHERE ri.scheduler_partition_id = $1
		   AND cr.status = 'unscheduled'
		 ORDER BY cr.workflow_id, cr.version DESC`,
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

// UpsertJobAndSchedule writes or overwrites the job for the workflow associated with requestID,
// but only if the request's version is strictly higher than the job currently in the table
// (and that job is still pending). Atomically marks the request as scheduled if written.
// The write is additionally guarded on the workflow's current_version still equaling
// expectedCurrentVersion — the run the caller validated against — so a stale validation
// (a newer run completed in between) results in written=false rather than scheduling.
// Returns written=false (no error) if any guard fails.
// contextJSON and currentAtomicOperation are assembled by the scheduler layer before calling.
func (r *SchedulerRepo) UpsertJobAndSchedule(ctx context.Context, requestID string, contextJSON string, currentAtomicOperation string, timeoutSeconds int, workflowVersion int, expectedCurrentVersion int64) (workflowID string, version int64, written bool, err error) {
	err = r.pool.QueryRow(ctx,
		`WITH candidate AS (
		     SELECT cr.id, cr.workflow_id, cr.version, cr.request_type,
		            ri.workflow_type, t.tenant_group_id
		     FROM customer_requests cr
		     JOIN workflows ri ON ri.id = cr.workflow_id
		     JOIN tenants t ON t.id = ri.tenant_id
		     WHERE cr.id = $1
		       AND ri.current_version = $6
		 ),
		 upserted AS (
		     INSERT INTO jobs (workflow_id, request_id, version, current_atomic_operation, context, status, job_type, workflow_type, timeout_seconds, workflow_version, tenant_group_id)
		     SELECT c.workflow_id, c.id, c.version, $2,
		            $3::jsonb,
		            'pending', c.request_type, c.workflow_type, $4, $5, c.tenant_group_id
		     FROM candidate c
		     ON CONFLICT (workflow_id) DO UPDATE
		         SET request_id               = EXCLUDED.request_id,
		             version                  = EXCLUDED.version,
		             current_atomic_operation = EXCLUDED.current_atomic_operation,
		             context                  = EXCLUDED.context,
		             job_type                 = EXCLUDED.job_type,
		             workflow_type            = EXCLUDED.workflow_type,
		             timeout_seconds          = EXCLUDED.timeout_seconds,
		             workflow_version         = EXCLUDED.workflow_version,
		             status                   = 'pending',
		             owner                    = NULL,
		             lease_expires_at         = NULL,
		             tenant_group_id          = EXCLUDED.tenant_group_id

		         WHERE jobs.version < EXCLUDED.version
		           AND jobs.status = 'pending'
		     RETURNING workflow_id, request_id
		 )
		 UPDATE customer_requests cr
		 SET status = 'scheduled'
		 FROM upserted u
		 WHERE cr.id = u.request_id
		 RETURNING cr.workflow_id, cr.version`,
		requestID, currentAtomicOperation, contextJSON, timeoutSeconds, workflowVersion, expectedCurrentVersion,
	).Scan(&workflowID, &version)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", 0, false, nil
		}
		return "", 0, false, fmt.Errorf("upsert job: %w", err)
	}
	return workflowID, version, true, nil
}

func (r *SchedulerRepo) GetCompletedJobs(ctx context.Context, partitionID string) ([]domain.CompletedJob, error) {
	rows, err := r.pool.Query(ctx,
		`SELECT j.request_id, j.result_type, j.failure_reason
		 FROM jobs j
		 JOIN workflows ri ON j.workflow_id = ri.id
		 WHERE ri.scheduler_partition_id = $1
		   AND j.status = 'completed'`,
		partitionID,
	)
	if err != nil {
		return nil, fmt.Errorf("get completed jobs: %w", err)
	}
	defer rows.Close()

	var jobs []domain.CompletedJob
	for rows.Next() {
		var j domain.CompletedJob
		if err := rows.Scan(&j.RequestID, &j.ResultType, &j.FailureReason); err != nil {
			return nil, fmt.Errorf("scan completed job: %w", err)
		}
		jobs = append(jobs, j)
	}
	return jobs, rows.Err()
}

func (r *SchedulerRepo) GetFailedJobRequestIDs(ctx context.Context, partitionID string) ([]string, error) {
	rows, err := r.pool.Query(ctx,
		`SELECT j.request_id
		 FROM jobs j
		 JOIN workflows ri ON j.workflow_id = ri.id
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

func (r *SchedulerRepo) DeleteTerminalJob(ctx context.Context, workflowID string, requestID string) (bool, error) {
	tag, err := r.pool.Exec(ctx,
		`DELETE FROM jobs
		 WHERE workflow_id = $1
		   AND request_id = $2
		   AND status IN ('completed', 'failed')`,
		workflowID, requestID,
	)
	if err != nil {
		return false, fmt.Errorf("delete terminal job: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

func (r *SchedulerRepo) MarkWorkflowAwaitingApproval(ctx context.Context, workflowID string) error {
	_, err := r.pool.Exec(ctx,
		`UPDATE workflows ri
		 SET lifecycle_state = 'awaiting_approval'
		 FROM jobs j
		 WHERE j.workflow_id = ri.id
		   AND j.status = 'awaiting_approval'
		   AND ri.id = $1`,
		workflowID,
	)
	if err != nil {
		return fmt.Errorf("mark workflow awaiting approval: %w", err)
	}
	return nil
}

func (r *SchedulerRepo) SyncAwaitingApprovalStates(ctx context.Context, partitionID string) ([]string, error) {
	rows, err := r.pool.Query(ctx,
		`UPDATE workflows ri
		 SET lifecycle_state = 'awaiting_approval'
		 FROM jobs j
		 WHERE j.workflow_id = ri.id
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

func (r *SchedulerRepo) SupercedeOlderRequests(ctx context.Context, workflowID string, version int64) error {
	_, err := r.pool.Exec(ctx,
		`UPDATE customer_requests
		 SET status = 'superceded'
		 WHERE workflow_id = $1
		   AND status IN ('unscheduled', 'scheduled')
		   AND version < $2`,
		workflowID, version,
	)
	if err != nil {
		return fmt.Errorf("supercede older requests: %w", err)
	}
	return nil
}
