package postgres

import (
	"context"
	"fmt"

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

// ListApprovals returns a tenant's approval audit events, most recent first.
// workflowID and approvalID are optional filters (empty string = no filter).
// Callers resolve each event with AuditEvent.ApprovalDetails().
func (r *AuditRepo) ListApprovals(ctx context.Context, tenantGroupID, workflowID, approvalID string) ([]domain.AuditEvent, error) {
	rows, err := r.pool.Query(ctx,
		`SELECT id, tenant_group_id, COALESCE(workflow_id, ''), COALESCE(request_id, ''),
		        event_type, actor, occurred_at, details, created_at
		 FROM audit_events
		 WHERE tenant_group_id = $1 AND event_type = $2
		   AND ($3 = '' OR workflow_id = $3)
		   AND ($4 = '' OR details->>'approval_id' = $4)
		 ORDER BY occurred_at DESC
		 LIMIT 500`,
		tenantGroupID, domain.AuditEventApproval, workflowID, approvalID)
	if err != nil {
		return nil, fmt.Errorf("list approval audit: %w", err)
	}
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
