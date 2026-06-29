package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/boundflow/boundflow/internal/domain"
)

type AuditRepo struct {
	pool *pgxpool.Pool
}

func NewAuditRepo(pool *pgxpool.Pool) *AuditRepo {
	return &AuditRepo{pool: pool}
}

// Append writes one audit event. Details must already be marshaled JSON.
func (r *AuditRepo) Append(ctx context.Context, e domain.AuditEvent) error {
	_, err := r.pool.Exec(ctx,
		`INSERT INTO audit_events (id, tenant_group_id, workflow_id, request_id, event_type, actor, occurred_at, details)
		 VALUES (gen_random_uuid(), $1, $2, $3, $4, $5, $6, $7)`,
		e.TenantGroupID, e.WorkflowID, e.RequestID, e.EventType, e.Actor, e.OccurredAt, e.Details)
	if err != nil {
		return fmt.Errorf("append audit event: %w", err)
	}
	return nil
}

// ListAuditEvents returns a tenant's audit events newest first. workflowID is an
// optional filter (empty = tenant-wide); agentName filters agent events to one agent
// (empty = no filter); eventTypes filters to those types (empty = all). Callers
// resolve each event by its EventType (ApprovalDetails / PolicyDetails / AgentPolicyDetails).
func (r *AuditRepo) ListAuditEvents(ctx context.Context, tenantGroupID, workflowID, agentName string, eventTypes []domain.AuditEventType) ([]domain.AuditEvent, error) {
	types := make([]string, len(eventTypes))
	for i, t := range eventTypes {
		types[i] = string(t)
	}
	rows, err := r.pool.Query(ctx,
		`SELECT id, tenant_group_id, COALESCE(workflow_id, ''), COALESCE(request_id, ''),
		        event_type, actor, occurred_at, details, created_at
		 FROM audit_events
		 WHERE tenant_group_id = $1
		   AND ($2 = '' OR workflow_id = $2)
		   AND ($3 = '' OR details->>'agent' = $3)
		   AND (cardinality($4::text[]) = 0 OR event_type = ANY($4))
		 ORDER BY occurred_at DESC
		 LIMIT 500`,
		tenantGroupID, workflowID, agentName, types)
	if err != nil {
		return nil, fmt.Errorf("list audit events: %w", err)
	}
	return scanAuditEvents(rows)
}

// GetApprovalByID returns the tenant's approval event with the given approval_id,
// or nil if none. Used for trace correlation (the trace carries approval_id).
func (r *AuditRepo) GetApprovalByID(ctx context.Context, tenantGroupID, approvalID string) (*domain.AuditEvent, error) {
	rows, err := r.pool.Query(ctx,
		`SELECT id, tenant_group_id, COALESCE(workflow_id, ''), COALESCE(request_id, ''),
		        event_type, actor, occurred_at, details, created_at
		 FROM audit_events
		 WHERE tenant_group_id = $1 AND event_type = $2 AND details->>'approval_id' = $3
		 ORDER BY occurred_at DESC
		 LIMIT 1`,
		tenantGroupID, domain.AuditEventApproval, approvalID)
	if err != nil {
		return nil, fmt.Errorf("get approval by id: %w", err)
	}
	events, err := scanAuditEvents(rows)
	if err != nil || len(events) == 0 {
		return nil, err
	}
	return &events[0], nil
}

func scanAuditEvents(rows pgx.Rows) ([]domain.AuditEvent, error) {
	defer rows.Close()
	var events []domain.AuditEvent
	for rows.Next() {
		var e domain.AuditEvent
		if err := rows.Scan(&e.ID, &e.TenantGroupID, &e.WorkflowID, &e.RequestID,
			&e.EventType, &e.Actor, &e.OccurredAt, &e.Details, &e.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan audit event: %w", err)
		}
		events = append(events, e)
	}
	return events, rows.Err()
}
