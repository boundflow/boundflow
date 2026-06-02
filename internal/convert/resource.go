package convert

import (
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	convergeplanev1 "github.com/convergeplane/convergeplane/gen/convergeplane/v1"
	"github.com/convergeplane/convergeplane/internal/domain"
)

func ResourceStateFromProto(s *structpb.Struct) domain.ResourceState {
	if s == nil {
		return nil
	}
	return domain.ResourceState(s.AsMap())
}

func ResourceStateToProto(s domain.ResourceState) *structpb.Struct {
	if s == nil {
		return nil
	}
	pb, _ := structpb.NewStruct(s)
	return pb
}

var workflowStateToProto = map[domain.WorkflowState]convergeplanev1.WorkflowState{
	domain.WorkflowStateActive:   convergeplanev1.WorkflowState_WORKFLOW_STATE_ACTIVE,
	domain.WorkflowStatePaused:   convergeplanev1.WorkflowState_WORKFLOW_STATE_PAUSED,
	domain.WorkflowStateCooldown: convergeplanev1.WorkflowState_WORKFLOW_STATE_COOLDOWN,
	domain.WorkflowStateDisabled: convergeplanev1.WorkflowState_WORKFLOW_STATE_DISABLED,
}

func ResourceInstanceToProto(r *domain.ResourceInstance) *convergeplanev1.ResourceInstance {
	if r == nil {
		return nil
	}
	return &convergeplanev1.ResourceInstance{
		Id:        r.ID,
		TenantId:  r.TenantID,
		CreatedAt: timestamppb.New(r.CreatedAt),
		WorkflowConfig: &convergeplanev1.WorkflowConfig{
			Version:              int32(r.CurrentWorkflowVersion),
			InvokeTimeoutSeconds: r.WorkflowConfig.InvokeTimeoutSeconds,
			RepeatEverySeconds:   r.WorkflowConfig.RepeatEverySeconds,
			Triggerable:          r.WorkflowConfig.Triggerable,
		},
		LifecycleState: string(r.LifecycleState),
		WorkflowState:  workflowStateToProto[r.WorkflowState],
	}
}

func WorkflowLifecyclePolicyFromProto(p *convergeplanev1.WorkflowLifecyclePolicy) domain.WorkflowLifecyclePolicy {
	if p == nil {
		return domain.WorkflowLifecyclePolicy{}
	}
	rules := make([]domain.WorkflowLifecyclePolicyRule, 0, len(p.Rules))
	for _, r := range p.Rules {
		if r == nil {
			continue
		}
		rules = append(rules, workflowRuleFromProto(r))
	}
	return domain.WorkflowLifecyclePolicy{Rules: rules}
}

func workflowRuleFromProto(r *convergeplanev1.WorkflowLifecyclePolicyRule) domain.WorkflowLifecyclePolicyRule {
	var metric domain.WorkflowMetric
	switch r.Metric {
	case convergeplanev1.WorkflowMetric_WORKFLOW_METRIC_COST:
		metric = domain.WorkflowMetricCost
	case convergeplanev1.WorkflowMetric_WORKFLOW_METRIC_NUM_LLM_CALLS:
		metric = domain.WorkflowMetricNumLLMCalls
	case convergeplanev1.WorkflowMetric_WORKFLOW_METRIC_LATENCY:
		metric = domain.WorkflowMetricLatency
	case convergeplanev1.WorkflowMetric_WORKFLOW_METRIC_APPROVAL_REJECTIONS:
		metric = domain.WorkflowMetricApprovalRejections
	case convergeplanev1.WorkflowMetric_WORKFLOW_METRIC_TOOL_FAILURE_RATE:
		metric = domain.WorkflowMetricToolFailureRate
	default:
		metric = domain.WorkflowMetricNumFailures
	}

	var action domain.WorkflowLifecyclePolicyAction
	if r.Action != nil {
		switch r.Action.Type {
		case convergeplanev1.WorkflowPolicyActionType_WORKFLOW_POLICY_ACTION_COOLDOWN:
			action = domain.WorkflowLifecyclePolicyAction{
				Type:            domain.WorkflowPolicyActionCooldown,
				CooldownSeconds: int(r.Action.CooldownSeconds),
			}
		case convergeplanev1.WorkflowPolicyActionType_WORKFLOW_POLICY_ACTION_SET_VERSION:
			action = domain.WorkflowLifecyclePolicyAction{
				Type:          domain.WorkflowPolicyActionSetVersion,
				TargetVersion: int(r.Action.TargetVersion),
			}
		default:
			action = domain.WorkflowLifecyclePolicyAction{Type: domain.WorkflowPolicyActionPause}
		}
	}

	return domain.WorkflowLifecyclePolicyRule{
		Metric:    metric,
		Threshold: r.Threshold,
		Window:    int(r.Window),
		ToolName:  r.ToolName,
		Action:    action,
	}
}

func WorkflowConfigFromProto(p *convergeplanev1.WorkflowConfig) domain.WorkflowConfig {
	if p == nil {
		return domain.WorkflowConfig{Triggerable: true}
	}
	return domain.WorkflowConfig{
		InvokeTimeoutSeconds: p.InvokeTimeoutSeconds,
		RepeatEverySeconds:   p.RepeatEverySeconds,
		Triggerable:          p.Triggerable,
	}
}
