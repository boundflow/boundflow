package domain

import (
	"encoding/json"
	"fmt"
	"time"
)

// AuditEventType discriminates the typed payload carried in AuditEvent.Details.
type AuditEventType string

const (
	AuditEventApproval AuditEventType = "approval"
	// AuditEventPolicyAction AuditEventType = "policy_action" // planned: lifecycle policy firings
)

// AuditEvent is one row of the governance audit log. The common query dimensions
// are fields; Details holds the type-specific payload, resolved per EventType on
// read (see ApprovalDetails).
type AuditEvent struct {
	ID            string
	TenantGroupID string
	WorkflowID    string
	RequestID     string
	EventType     AuditEventType
	Actor         string
	OccurredAt    time.Time
	Details       json.RawMessage
	CreatedAt     time.Time
}

// ApprovalDecision is how an approval gate resolved.
type ApprovalDecision string

const (
	ApprovalApproved ApprovalDecision = "approved"
	ApprovalRejected ApprovalDecision = "rejected"
	ApprovalTimedOut ApprovalDecision = "timed_out"
)

// ApprovalAuditDetails is the typed payload of an AuditEventApproval event.
type ApprovalAuditDetails struct {
	ApprovalID string           `json:"approval_id"`
	OpenedAt   *time.Time       `json:"opened_at,omitempty"`
	DecidedAt  *time.Time       `json:"decided_at,omitempty"`
	Decision   ApprovalDecision `json:"decision"`
}

// ApprovalDetails resolves the event's Details as an approval record. It errors if
// the event is not an approval event.
func (e AuditEvent) ApprovalDetails() (*ApprovalAuditDetails, error) {
	if e.EventType != AuditEventApproval {
		return nil, fmt.Errorf("audit event %s is %s, not an approval event", e.ID, e.EventType)
	}
	var d ApprovalAuditDetails
	if err := json.Unmarshal(e.Details, &d); err != nil {
		return nil, fmt.Errorf("unmarshal approval audit details: %w", err)
	}
	return &d, nil
}
