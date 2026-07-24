package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/boundflow/boundflow/internal/domain"
	"github.com/boundflow/boundflow/internal/storage"
)

type CustomerRequestRepo struct {
	pool *pgxpool.Pool
}

func NewCustomerRequestRepo(pool *pgxpool.Pool) *CustomerRequestRepo {
	return &CustomerRequestRepo{pool: pool}
}

func (r *CustomerRequestRepo) Create(ctx context.Context, req *domain.CustomerRequest) error {
	requestInfo, err := json.Marshal(req.RequestInfo)
	if err != nil {
		return fmt.Errorf("marshal request info: %w", err)
	}

	_, err = r.pool.Exec(ctx,
		`INSERT INTO customer_requests (id, workflow_id, status, request_type, request_info, version, created_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		req.ID, req.WorkflowID,
		req.Status, req.RequestType, requestInfo, req.Version, req.CreatedAt,
	)
	if err != nil {
		return handleError(err, "customer request")
	}
	return nil
}

// CreateInvocationRequest atomically allocates the next version (bumping the workflow's
// target_version) and inserts the request with that version — all in one statement. Fails
// with ErrInvalidLifecycleState if the workflow is in one of invalidStates. Sets req.Version
// and returns the allocated version. (The lifecycle_state -> invoking transition happens
// best-effort in ScheduleRequest, once the job actually lands on the jobs table.)
func (r *CustomerRequestRepo) CreateInvocationRequest(ctx context.Context, req *domain.CustomerRequest, invalidStates []domain.LifecycleState) (int64, error) {
	requestInfo, err := json.Marshal(req.RequestInfo)
	if err != nil {
		return 0, fmt.Errorf("marshal request info: %w", err)
	}
	invalid := lifecycleStateStrings(invalidStates)

	var version int64
	err = r.pool.QueryRow(ctx,
		`WITH bumped AS (
		     UPDATE workflows
		     SET target_version = target_version + 1
		     WHERE id = $2 AND NOT (lifecycle_state = ANY($6::lifecycle_state[]))
		     RETURNING target_version
		 )
		 INSERT INTO customer_requests (id, workflow_id, status, request_type, request_info, version, created_at)
		 SELECT $1, $2, $3, $4, $5, b.target_version, now()
		 FROM bumped b
		 RETURNING version`,
		req.ID, req.WorkflowID, req.Status, req.RequestType, requestInfo, invalid,
	).Scan(&version)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, storage.ErrInvalidLifecycleState
		}
		return 0, handleError(err, "customer request")
	}
	req.Version = version
	return version, nil
}

// CreateDuePeriodicRequest is CreateInvocationRequest gated by the periodic guards: it only
// allocates+inserts if the workflow is not in invalidStates, has no non-terminal request in
// flight, and its most recent terminal request completed at least minGap ago. Returns
// created=false (no error) when any guard rejects. The whole thing is one atomic statement, so
// the version bump only happens when the request is actually created.
func (r *CustomerRequestRepo) CreateDuePeriodicRequest(ctx context.Context, req *domain.CustomerRequest, minGap time.Duration, invalidStates []domain.LifecycleState) (int64, bool, error) {
	requestInfo, err := json.Marshal(req.RequestInfo)
	if err != nil {
		return 0, false, fmt.Errorf("marshal request info: %w", err)
	}
	invalid := lifecycleStateStrings(invalidStates)

	var version int64
	err = r.pool.QueryRow(ctx,
		`WITH bumped AS (
		     UPDATE workflows ri
		     SET target_version = target_version + 1
		     WHERE ri.id = $2
		       AND NOT (ri.lifecycle_state = ANY($6::lifecycle_state[]))
		       AND NOT EXISTS (
		           SELECT 1 FROM customer_requests cr
		           WHERE cr.workflow_id = ri.id
		             AND cr.status NOT IN ('completed', 'failed', 'superceded')
		       )
		       AND COALESCE(
		               (SELECT max(cr.completed_at) FROM customer_requests cr WHERE cr.workflow_id = ri.id),
		               'epoch'::timestamptz
		           ) + make_interval(secs => $7) < now()
		     RETURNING target_version
		 )
		 INSERT INTO customer_requests (id, workflow_id, status, request_type, request_info, version, created_at)
		 SELECT $1, $2, $3, $4, $5, b.target_version, now()
		 FROM bumped b
		 RETURNING version`,
		req.ID, req.WorkflowID, req.Status, req.RequestType, requestInfo, invalid, minGap.Seconds(),
	).Scan(&version)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, false, nil
		}
		return 0, false, handleError(err, "customer request")
	}
	req.Version = version
	return version, true, nil
}

func lifecycleStateStrings(states []domain.LifecycleState) []string {
	out := make([]string, len(states))
	for i, s := range states {
		out[i] = string(s)
	}
	return out
}

// AbandonUnscheduledRequests fails every unscheduled request for the workflow. Safe to
// call repeatedly - the periodic reconciler calls it again for any workflow still
// waiting to finalize deletion.
func (r *CustomerRequestRepo) AbandonUnscheduledRequests(ctx context.Context, workflowID string) error {
	_, err := r.pool.Exec(ctx,
		`UPDATE customer_requests SET status = 'abandoned' WHERE workflow_id = $1 AND status = 'unscheduled'`,
		workflowID,
	)
	if err != nil {
		return fmt.Errorf("abandon unscheduled requests: %w", err)
	}
	return nil
}

// HasRunningRequest reports whether the workflow currently has a scheduled or
// in-progress request.
func (r *CustomerRequestRepo) HasRunningRequest(ctx context.Context, workflowID string) (bool, error) {
	var running bool
	err := r.pool.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM customer_requests WHERE workflow_id = $1 AND status IN ('scheduled', 'in_progress'))`,
		workflowID,
	).Scan(&running)
	if err != nil {
		return false, fmt.Errorf("check for running request: %w", err)
	}
	return running, nil
}

func (r *CustomerRequestRepo) CountUnscheduledRequests(ctx context.Context, workflowID string) (int, error) {
	var n int
	err := r.pool.QueryRow(ctx,
		`SELECT count(*) FROM customer_requests WHERE workflow_id = $1 AND status = 'unscheduled'`,
		workflowID,
	).Scan(&n)
	if err != nil {
		return 0, fmt.Errorf("count unscheduled requests: %w", err)
	}
	return n, nil
}

func (r *CustomerRequestRepo) Get(ctx context.Context, id string) (*domain.CustomerRequest, error) {
	var req domain.CustomerRequest
	var requestInfoJSON, resultJSON []byte

	err := r.pool.QueryRow(ctx,
		`SELECT id, workflow_id, status, request_type, request_info, version,
		        COALESCE(run_outcome::text, ''), failure_reason, created_at, completed_at, result
		 FROM customer_requests WHERE id = $1`,
		id,
	).Scan(&req.ID, &req.WorkflowID, &req.Status, &req.RequestType, &requestInfoJSON, &req.Version,
		&req.RunOutcome, &req.FailureReason, &req.CreatedAt, &req.CompletedAt, &resultJSON)
	if err != nil {
		return nil, handleError(err, "customer request")
	}
	if err := json.Unmarshal(requestInfoJSON, &req.RequestInfo); err != nil {
		return nil, fmt.Errorf("unmarshal request info: %w", err)
	}
	if resultJSON != nil {
		if err := json.Unmarshal(resultJSON, &req.Result); err != nil {
			return nil, fmt.Errorf("unmarshal result: %w", err)
		}
	}
	return &req, nil
}

func (r *CustomerRequestRepo) CompleteRequest(ctx context.Context, id string, outcome domain.RunOutcome, failureReason string, result map[string]any) (*domain.CustomerRequest, error) {
	var req domain.CustomerRequest
	var requestInfoJSON, resultJSON []byte
	var resultParam any
	if result != nil {
		marshaled, err := json.Marshal(result)
		if err != nil {
			return nil, fmt.Errorf("marshal result: %w", err)
		}
		resultParam = marshaled
	}

	err := r.pool.QueryRow(ctx,
		`UPDATE customer_requests SET status = $1, run_outcome = $2, failure_reason = $3, result = $4, completed_at = now() WHERE id = $5
		 RETURNING id, workflow_id, status, request_type, request_info, version, created_at, completed_at, result`,
		domain.CustomerRequestStatusCompleted, outcome, failureReason, resultParam, id,
	).Scan(&req.ID, &req.WorkflowID, &req.Status, &req.RequestType, &requestInfoJSON, &req.Version, &req.CreatedAt, &req.CompletedAt, &resultJSON)
	if err != nil {
		return nil, handleError(err, "customer request")
	}
	if err := json.Unmarshal(requestInfoJSON, &req.RequestInfo); err != nil {
		return nil, fmt.Errorf("unmarshal request info: %w", err)
	}
	if resultJSON != nil {
		if err := json.Unmarshal(resultJSON, &req.Result); err != nil {
			return nil, fmt.Errorf("unmarshal result: %w", err)
		}
	}
	return &req, nil
}

func (r *CustomerRequestRepo) FailRequest(ctx context.Context, id string, failureReason string) (*domain.CustomerRequest, error) {
	var req domain.CustomerRequest
	var requestInfoJSON []byte

	// A failed request is always the platform-failure outcome (interrupted).
	err := r.pool.QueryRow(ctx,
		`UPDATE customer_requests SET status = $1, run_outcome = $2, failure_reason = $3, completed_at = now() WHERE id = $4
		 RETURNING id, workflow_id, status, request_type, request_info, version, created_at, completed_at`,
		domain.CustomerRequestStatusFailed, domain.RunOutcomeInterrupted, failureReason, id,
	).Scan(&req.ID, &req.WorkflowID, &req.Status, &req.RequestType, &requestInfoJSON, &req.Version, &req.CreatedAt, &req.CompletedAt)
	if err != nil {
		return nil, handleError(err, "customer request")
	}
	if err := json.Unmarshal(requestInfoJSON, &req.RequestInfo); err != nil {
		return nil, fmt.Errorf("unmarshal request info: %w", err)
	}
	return &req, nil
}

// ListForWorkflow returns every run (request) for a workflow, newest first. run_outcome
// is empty while a run is still in flight (NULL in the DB).
func (r *CustomerRequestRepo) ListForWorkflow(ctx context.Context, workflowID string) ([]*domain.CustomerRequest, error) {
	rows, err := r.pool.Query(ctx,
		`SELECT id, workflow_id, status, request_type, request_info, version,
		        COALESCE(run_outcome::text, ''), failure_reason, created_at, completed_at
		 FROM customer_requests WHERE workflow_id = $1 ORDER BY created_at DESC`,
		workflowID,
	)
	if err != nil {
		return nil, handleError(err, "customer request")
	}
	defer rows.Close()

	var out []*domain.CustomerRequest
	for rows.Next() {
		var req domain.CustomerRequest
		var requestInfoJSON []byte
		if err := rows.Scan(
			&req.ID, &req.WorkflowID, &req.Status, &req.RequestType, &requestInfoJSON, &req.Version,
			&req.RunOutcome, &req.FailureReason, &req.CreatedAt, &req.CompletedAt,
		); err != nil {
			return nil, handleError(err, "customer request")
		}
		if err := json.Unmarshal(requestInfoJSON, &req.RequestInfo); err != nil {
			return nil, fmt.Errorf("unmarshal request info: %w", err)
		}
		out = append(out, &req)
	}
	return out, rows.Err()
}

func (r *CustomerRequestRepo) UpdateStatus(ctx context.Context, workflowID, id string, status domain.CustomerRequestStatus) error {
	_, err := r.pool.Exec(ctx,
		`UPDATE customer_requests SET status = $1 WHERE workflow_id = $2 AND id = $3`,
		status, workflowID, id,
	)
	if err != nil {
		return fmt.Errorf("update customer request status: %w", err)
	}
	return nil
}

