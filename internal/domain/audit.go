package domain

import (
	"encoding/json"
	"fmt"
	"time"
)

// AuditEventType discriminates the typed payload carried in AuditEvent.Details.
type AuditEventType string

const (
	AuditEventApproval     AuditEventType = "approval"
	AuditEventPolicyAction AuditEventType = "policy_action"
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

// PolicyActionDetails is the typed payload of an AuditEventPolicyAction event — a
// lifecycle policy rule that fired and changed workflow state. The rule itself is
// embedded (it carries metric/threshold/window/tool + the action with its target
// version / cooldown), so the record is self-describing. Only the value that
// crossed and the prior state aren't in the rule; the resulting state is the rule's
// action applied to that prior state.
type PolicyActionDetails struct {
	Scope           string                      `json:"scope"` // "workflow" (agent scope planned)
	Rule            WorkflowLifecyclePolicyRule `json:"rule"`
	TriggerValue    float64                     `json:"trigger_value"`
	PreviousVersion int                         `json:"previous_version"`
	PreviousState   WorkflowState               `json:"previous_state"`
}

// PolicyDetails resolves the event's Details as a policy-action record. It errors if
// the event is not a policy_action event.
func (e AuditEvent) PolicyDetails() (*PolicyActionDetails, error) {
	if e.EventType != AuditEventPolicyAction {
		return nil, fmt.Errorf("audit event %s is %s, not a policy_action event", e.ID, e.EventType)
	}
	var d PolicyActionDetails
	if err := json.Unmarshal(e.Details, &d); err != nil {
		return nil, fmt.Errorf("unmarshal policy action details: %w", err)
	}
	return &d, nil
}
