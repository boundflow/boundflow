package postgres

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/convergeplane/convergeplane/internal/domain"
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
		`UPDATE customer_requests SET status = $1 WHERE id = $2
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
		`UPDATE customer_requests SET status = $1 WHERE id = $2
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

