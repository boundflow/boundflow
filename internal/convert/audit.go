package convert

import (
	"fmt"

	"google.golang.org/protobuf/types/known/timestamppb"

	boundflowv1 "github.com/boundflow/boundflow/gen/boundflow/v1"
	"github.com/boundflow/boundflow/internal/domain"
)

// ApprovalAuditRecord builds the proto record from an approval audit event.
func ApprovalAuditRecord(e domain.AuditEvent) (*boundflowv1.ApprovalAuditRecord, error) {
	d, err := e.ApprovalDetails()
	if err != nil {
		return nil, err
	}
	rec := &boundflowv1.ApprovalAuditRecord{
		ApprovalId: d.ApprovalID,
		WorkflowId: e.WorkflowID,
		RequestId:  e.RequestID,
		Decision:   approvalDecisionToProto(d.Decision),
		Actor:      e.Actor,
		OccurredAt: timestamppb.New(e.OccurredAt),
	}
	if d.OpenedAt != nil {
		rec.OpenedAt = timestamppb.New(*d.OpenedAt)
	}
	if d.DecidedAt != nil {
		rec.DecidedAt = timestamppb.New(*d.DecidedAt)
	}
	return rec, nil
}

// WorkflowPolicyAuditRecord builds the proto record from a workflow policy_action event.
func WorkflowPolicyAuditRecord(e domain.AuditEvent) (*boundflowv1.WorkflowPolicyAuditRecord, error) {
	d, err := e.PolicyDetails()
	if err != nil {
		return nil, err
	}
	return &boundflowv1.WorkflowPolicyAuditRecord{
		WorkflowId:      e.WorkflowID,
		RequestId:       e.RequestID,
		Actor:           e.Actor,
		OccurredAt:      timestamppb.New(e.OccurredAt),
		Rule:            WorkflowRuleToProto(d.Rule),
		TriggerValue:    d.TriggerValue,
		PreviousVersion: int32(d.PreviousVersion),
		PreviousState:   string(d.PreviousState),
	}, nil
}

// AgentPolicyAuditRecord builds the proto record from an agent_policy_action event.
func AgentPolicyAuditRecord(e domain.AuditEvent) (*boundflowv1.AgentPolicyAuditRecord, error) {
	d, err := e.AgentPolicyDetails()
	if err != nil {
		return nil, err
	}
	return &boundflowv1.AgentPolicyAuditRecord{
		WorkflowId: e.WorkflowID,
		RequestId:  e.RequestID,
		Actor:      e.Actor,
		OccurredAt: timestamppb.New(e.OccurredAt),
		AgentName:  d.Agent,
		Action:     AgentPolicyActionToProto(*d),
	}, nil
}

// AuditEntry maps an audit event to a unified-log entry (oneof by event type).
func AuditEntry(e domain.AuditEvent) (*boundflowv1.AuditEntry, error) {
	switch e.EventType {
	case domain.AuditEventApproval:
		rec, err := ApprovalAuditRecord(e)
		if err != nil {
			return nil, err
		}
		return &boundflowv1.AuditEntry{Entry: &boundflowv1.AuditEntry_Approval{Approval: rec}}, nil
	case domain.AuditEventPolicyAction:
		rec, err := WorkflowPolicyAuditRecord(e)
		if err != nil {
			return nil, err
		}
		return &boundflowv1.AuditEntry{Entry: &boundflowv1.AuditEntry_WorkflowPolicy{WorkflowPolicy: rec}}, nil
	case domain.AuditEventAgentPolicyAction:
		rec, err := AgentPolicyAuditRecord(e)
		if err != nil {
			return nil, err
		}
		return &boundflowv1.AuditEntry{Entry: &boundflowv1.AuditEntry_AgentPolicy{AgentPolicy: rec}}, nil
	}
	return nil, fmt.Errorf("unknown audit event type %q", e.EventType)
}

func approvalDecisionToProto(d domain.ApprovalDecision) boundflowv1.ApprovalDecision {
	switch d {
	case domain.ApprovalApproved:
		return boundflowv1.ApprovalDecision_APPROVAL_DECISION_APPROVED
	case domain.ApprovalRejected:
		return boundflowv1.ApprovalDecision_APPROVAL_DECISION_REJECTED
	case domain.ApprovalTimedOut:
		return boundflowv1.ApprovalDecision_APPROVAL_DECISION_TIMED_OUT
	}
	return boundflowv1.ApprovalDecision_APPROVAL_DECISION_UNSPECIFIED
}
