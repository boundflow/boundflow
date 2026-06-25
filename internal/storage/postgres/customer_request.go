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
		`INSERT INTO customer_requests (id, resource_instance_id, status, request_type, request_info, version, created_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		req.ID, req.ResourceInstanceID,
		req.Status, req.RequestType, requestInfo, req.Version, req.CreatedAt,
	)
	if err != nil {
		return handleError(err, "customer request")
	}
	return nil
}

// CreateInvocationRequest atomically allocates the next version (bumping the resource's
// target_version), flips lifecycle_state to reconciling, and inserts the request with that
// version — all in one statement. Fails with ErrInvalidLifecycleState if the resource is in
// one of invalidStates. Sets req.Version and returns the allocated version.
func (r *CustomerRequestRepo) CreateInvocationRequest(ctx context.Context, req *domain.CustomerRequest, invalidStates []domain.LifecycleState) (int64, error) {
	requestInfo, err := json.Marshal(req.RequestInfo)
	if err != nil {
		return 0, fmt.Errorf("marshal request info: %w", err)
	}
	invalid := lifecycleStateStrings(invalidStates)

	var version int64
	err = r.pool.QueryRow(ctx,
		`WITH bumped AS (
		     UPDATE resource_instances
		     SET target_version = target_version + 1, lifecycle_state = 'reconciling'
		     WHERE id = $2 AND NOT (lifecycle_state = ANY($6::lifecycle_state[]))
		     RETURNING target_version
		 )
		 INSERT INTO customer_requests (id, resource_instance_id, status, request_type, request_info, version, created_at)
		 SELECT $1, $2, $3, $4, $5, b.target_version, now()
		 FROM bumped b
		 RETURNING version`,
		req.ID, req.ResourceInstanceID, req.Status, req.RequestType, requestInfo, invalid,
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
// allocates+inserts if the resource is not in invalidStates, has no non-terminal request in
// flight, and its most recent terminal request completed at least minGap ago. Returns
// created=false (no error) when any guard rejects. The whole thing is one atomic statement, so
// the version bump / state flip only happen when the request is actually created.
func (r *CustomerRequestRepo) CreateDuePeriodicRequest(ctx context.Context, req *domain.CustomerRequest, minGap time.Duration, invalidStates []domain.LifecycleState) (int64, bool, error) {
	requestInfo, err := json.Marshal(req.RequestInfo)
	if err != nil {
		return 0, false, fmt.Errorf("marshal request info: %w", err)
	}
	invalid := lifecycleStateStrings(invalidStates)

	var version int64
	err = r.pool.QueryRow(ctx,
		`WITH bumped AS (
		     UPDATE resource_instances ri
		     SET target_version = target_version + 1, lifecycle_state = 'reconciling'
		     WHERE ri.id = $2
		       AND NOT (ri.lifecycle_state = ANY($6::lifecycle_state[]))
		       AND NOT EXISTS (
		           SELECT 1 FROM customer_requests cr
		           WHERE cr.resource_instance_id = ri.id
		             AND cr.status NOT IN ('completed', 'failed', 'superceded')
		       )
		       AND COALESCE(
		               (SELECT max(cr.completed_at) FROM customer_requests cr WHERE cr.resource_instance_id = ri.id),
		               'epoch'::timestamptz
		           ) + make_interval(secs => $7) < now()
		     RETURNING target_version
		 )
		 INSERT INTO customer_requests (id, resource_instance_id, status, request_type, request_info, version, created_at)
		 SELECT $1, $2, $3, $4, $5, b.target_version, now()
		 FROM bumped b
		 RETURNING version`,
		req.ID, req.ResourceInstanceID, req.Status, req.RequestType, requestInfo, invalid, minGap.Seconds(),
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

func (r *CustomerRequestRepo) Get(ctx context.Context, id string) (*domain.CustomerRequest, error) {
	var req domain.CustomerRequest
	var requestInfoJSON []byte

	err := r.pool.QueryRow(ctx,
		`SELECT id, resource_instance_id, status, request_type, request_info, version, created_at
		 FROM customer_requests WHERE id = $1`,
		id,
	).Scan(&req.ID, &req.ResourceInstanceID, &req.Status, &req.RequestType, &requestInfoJSON, &req.Version, &req.CreatedAt)
	if err != nil {
		return nil, handleError(err, "customer request")
	}
	if err := json.Unmarshal(requestInfoJSON, &req.RequestInfo); err != nil {
		return nil, fmt.Errorf("unmarshal request info: %w", err)
	}
	return &req, nil
}

func (r *CustomerRequestRepo) CompleteRequest(ctx context.Context, id string) (*domain.CustomerRequest, error) {
	var req domain.CustomerRequest
	var requestInfoJSON []byte

	err := r.pool.QueryRow(ctx,
		`UPDATE customer_requests SET status = $1, completed_at = now() WHERE id = $2
		 RETURNING id, resource_instance_id, status, request_type, request_info, version, created_at`,
		domain.CustomerRequestStatusCompleted, id,
	).Scan(&req.ID, &req.ResourceInstanceID, &req.Status, &req.RequestType, &requestInfoJSON, &req.Version, &req.CreatedAt)
	if err != nil {
		return nil, handleError(err, "customer request")
	}
	if err := json.Unmarshal(requestInfoJSON, &req.RequestInfo); err != nil {
		return nil, fmt.Errorf("unmarshal request info: %w", err)
	}
	return &req, nil
}

func (r *CustomerRequestRepo) FailRequest(ctx context.Context, id string) (*domain.CustomerRequest, error) {
	var req domain.CustomerRequest
	var requestInfoJSON []byte

	err := r.pool.QueryRow(ctx,
		`UPDATE customer_requests SET status = $1, completed_at = now() WHERE id = $2
		 RETURNING id, resource_instance_id, status, request_type, request_info, version, created_at`,
		domain.CustomerRequestStatusFailed, id,
	).Scan(&req.ID, &req.ResourceInstanceID, &req.Status, &req.RequestType, &requestInfoJSON, &req.Version, &req.CreatedAt)
	if err != nil {
		return nil, handleError(err, "customer request")
	}
	if err := json.Unmarshal(requestInfoJSON, &req.RequestInfo); err != nil {
		return nil, fmt.Errorf("unmarshal request info: %w", err)
	}
	return &req, nil
}

func (r *CustomerRequestRepo) UpdateStatus(ctx context.Context, resourceInstanceID, id string, status domain.CustomerRequestStatus) error {
	_, err := r.pool.Exec(ctx,
		`UPDATE customer_requests SET status = $1 WHERE resource_instance_id = $2 AND id = $3`,
		status, resourceInstanceID, id,
	)
	if err != nil {
		return fmt.Errorf("update customer request status: %w", err)
	}
	return nil
}

