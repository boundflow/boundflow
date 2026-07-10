package domain

import (
	"encoding/json"
	"fmt"
	"time"
)

// AuditEventType discriminates the typed payload carried in AuditEvent.Details.
type AuditEventType string

const (
	AuditEventApproval          AuditEventType = "approval"
	AuditEventPolicyAction      AuditEventType = "policy_action"       // workflow lifecycle policy
	AuditEventAgentPolicyAction AuditEventType = "agent_policy_action" // agent lifecycle policy
	AuditEventInput             AuditEventType = "input"
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

// InputDecision is how an input gate resolved.
type InputDecision string

const (
	InputAnswered InputDecision = "answered"
	InputTimedOut InputDecision = "timed_out"
)

// InputAuditDetails is the typed payload of an AuditEventInput event. Answer is the
// submitted content — recorded here (not just threaded into the resumed context)
// because it's the governance-relevant content, same as an approval decision.
type InputAuditDetails struct {
	InputID   string         `json:"input_id"`
	OpenedAt  *time.Time     `json:"opened_at,omitempty"`
	DecidedAt *time.Time     `json:"decided_at,omitempty"`
	Decision  InputDecision  `json:"decision"`
	Answer    map[string]any `json:"answer,omitempty"`
}

// InputDetails resolves the event's Details as an input record. It errors if the
// event is not an input event.
func (e AuditEvent) InputDetails() (*InputAuditDetails, error) {
	if e.EventType != AuditEventInput {
		return nil, fmt.Errorf("audit event %s is %s, not an input event", e.ID, e.EventType)
	}
	var d InputAuditDetails
	if err := json.Unmarshal(e.Details, &d); err != nil {
		return nil, fmt.Errorf("unmarshal input audit details: %w", err)
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

// PolicyDetails resolves the event's Details as a workflow policy-action record. It
// errors if the event is not a policy_action event.
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

// AgentPolicyDetails resolves the event's Details as an agent policy-action record.
func (e AuditEvent) AgentPolicyDetails() (*AgentPolicyActionDetails, error) {
	if e.EventType != AuditEventAgentPolicyAction {
		return nil, fmt.Errorf("audit event %s is %s, not an agent_policy_action event", e.ID, e.EventType)
	}
	var d AgentPolicyActionDetails
	if err := json.Unmarshal(e.Details, &d); err != nil {
		return nil, fmt.Errorf("unmarshal agent policy action details: %w", err)
	}
	return &d, nil
}
