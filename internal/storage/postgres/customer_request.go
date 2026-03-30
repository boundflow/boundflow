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

	currentSnapshot, err := json.Marshal(req.CurrentConfigSnapshot)
	if err != nil {
		return fmt.Errorf("marshal current config snapshot: %w", err)
	}

	goalSnapshot, err := json.Marshal(req.GoalConfigSnapshot)
	if err != nil {
		return fmt.Errorf("marshal goal config snapshot: %w", err)
	}

	_, err = r.pool.Exec(ctx,
		`INSERT INTO customer_requests (id, resource_instance_id, superceded_request_id, status, request_type, request_info, current_config_snapshot, goal_config_snapshot, version, created_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
		req.ID, req.ResourceInstanceID, nilIfEmpty(req.SupercededRequestID),
		req.Status, req.RequestType, requestInfo, currentSnapshot, goalSnapshot, req.Version, req.CreatedAt,
	)
	if err != nil {
		return handleError(err, "customer request")
	}
	return nil
}

func (r *CustomerRequestRepo) Get(ctx context.Context, resourceInstanceID, id string) (*domain.CustomerRequest, error) {
	var req domain.CustomerRequest
	var requestInfoJSON, currentSnapshotJSON, goalSnapshotJSON []byte
	var supercededID *string

	err := r.pool.QueryRow(ctx,
		`SELECT id, resource_instance_id, superceded_request_id, status, request_type, request_info, current_config_snapshot, goal_config_snapshot, version, created_at
		 FROM customer_requests WHERE resource_instance_id = $1 AND id = $2`,
		resourceInstanceID, id,
	).Scan(
		&req.ID, &req.ResourceInstanceID, &supercededID,
		&req.Status, &req.RequestType, &requestInfoJSON, &currentSnapshotJSON, &goalSnapshotJSON, &req.Version, &req.CreatedAt,
	)
	if err != nil {
		return nil, handleError(err, "customer request")
	}

	if supercededID != nil {
		req.SupercededRequestID = *supercededID
	}

	if err := json.Unmarshal(requestInfoJSON, &req.RequestInfo); err != nil {
		return nil, fmt.Errorf("unmarshal request info: %w", err)
	}
	if err := json.Unmarshal(currentSnapshotJSON, &req.CurrentConfigSnapshot); err != nil {
		return nil, fmt.Errorf("unmarshal current config snapshot: %w", err)
	}
	if err := json.Unmarshal(goalSnapshotJSON, &req.GoalConfigSnapshot); err != nil {
		return nil, fmt.Errorf("unmarshal goal config snapshot: %w", err)
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

func (r *CustomerRequestRepo) UpdateSupercededBy(ctx context.Context, resourceInstanceID, id string, supercededRequestID string) error {
	_, err := r.pool.Exec(ctx,
		`UPDATE customer_requests SET superceded_request_id = $1, status = $2 WHERE resource_instance_id = $3 AND id = $4`,
		supercededRequestID, domain.CustomerRequestStatusSuperceded, resourceInstanceID, id,
	)
	if err != nil {
		return fmt.Errorf("update superceded request: %w", err)
	}
	return nil
}

func nilIfEmpty(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}
