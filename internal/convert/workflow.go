package convert

import (
	"google.golang.org/protobuf/types/known/timestamppb"

	boundflowv1 "github.com/boundflow/boundflow/gen/boundflow/v1"
	"github.com/boundflow/boundflow/internal/domain"
)

var workflowStateToProto = map[domain.WorkflowState]boundflowv1.WorkflowState{
	domain.WorkflowStateActive:   boundflowv1.WorkflowState_WORKFLOW_STATE_ACTIVE,
	domain.WorkflowStatePaused:   boundflowv1.WorkflowState_WORKFLOW_STATE_PAUSED,
	domain.WorkflowStateCooldown: boundflowv1.WorkflowState_WORKFLOW_STATE_COOLDOWN,
	domain.WorkflowStateDisabled: boundflowv1.WorkflowState_WORKFLOW_STATE_DISABLED,
}

func WorkflowToProto(r *domain.Workflow) *boundflowv1.Workflow {
	if r == nil {
		return nil
	}
	return &boundflowv1.Workflow{
		Id:           r.ID,
		WorkflowType: r.WorkflowType,
		TenantId:     r.TenantID,
		CreatedAt:    timestamppb.New(r.CreatedAt),
		WorkflowConfig: &boundflowv1.WorkflowConfig{
			Version:              int32(r.CurrentWorkflowVersion),
			InvokeTimeoutSeconds: r.WorkflowConfig.InvokeTimeoutSeconds,
			RepeatEverySeconds:   r.WorkflowConfig.RepeatEverySeconds,
			Triggerable:          r.WorkflowConfig.Triggerable,
			InvokeMode:           invokeModeToProto(r.WorkflowConfig.InvokeMode),
			MaxQueueDepth:        r.WorkflowConfig.MaxQueueDepth,
		},
		LifecycleState:      string(r.LifecycleState),
		WorkflowState:       workflowStateToProto[r.WorkflowState],
		LastInterruptedRequestId: r.LastInterruptedRequestID,
	}
}

// RunToProto maps a customer request (a run) to the runs-API Run message. run_outcome
// is empty while the run is still in flight.
func RunToProto(r *domain.CustomerRequest) *boundflowv1.Run {
	if r == nil {
		return nil
	}
	run := &boundflowv1.Run{
		RequestId:     r.ID,
		RequestType:   string(r.RequestType),
		Status:        string(r.Status),
		RunOutcome:    string(r.RunOutcome),
		FailureReason: r.FailureReason,
		CreatedAt:     timestamppb.New(r.CreatedAt),
	}
	if r.CompletedAt != nil {
		run.CompletedAt = timestamppb.New(*r.CompletedAt)
	}
	return run
}

// RequestInfoToProto maps a customer request to the GetRequestInfo message.
func RequestInfoToProto(r *domain.CustomerRequest) *boundflowv1.RequestInfo {
	if r == nil {
		return nil
	}
	info := &boundflowv1.RequestInfo{
		RequestId:     r.ID,
		WorkflowId:    r.WorkflowID,
		RequestType:   string(r.RequestType),
		Status:        string(r.Status),
		RunOutcome:    string(r.RunOutcome),
		FailureReason: r.FailureReason,
		Version:       r.Version,
		CreatedAt:     timestamppb.New(r.CreatedAt),
	}
	if r.CompletedAt != nil {
		info.CompletedAt = timestamppb.New(*r.CompletedAt)
	}
	return info
}

func WorkflowLifecyclePolicyFromProto(p *boundflowv1.WorkflowLifecyclePolicy) domain.WorkflowLifecyclePolicy {
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

// WorkflowLifecyclePolicyToProto is the inverse of WorkflowLifecyclePolicyFromProto,
// used by the policy getter to echo the armed policy back to the caller.
func WorkflowLifecyclePolicyToProto(p domain.WorkflowLifecyclePolicy) *boundflowv1.WorkflowLifecyclePolicy {
	rules := make([]*boundflowv1.WorkflowLifecyclePolicyRule, 0, len(p.Rules))
	for _, r := range p.Rules {
		rules = append(rules, WorkflowRuleToProto(r))
	}
	return &boundflowv1.WorkflowLifecyclePolicy{Rules: rules}
}

func workflowRuleFromProto(r *boundflowv1.WorkflowLifecyclePolicyRule) domain.WorkflowLifecyclePolicyRule {
	var metric domain.WorkflowMetric
	switch r.Metric {
	case boundflowv1.WorkflowMetric_WORKFLOW_METRIC_COST:
		metric = domain.WorkflowMetricCost
	case boundflowv1.WorkflowMetric_WORKFLOW_METRIC_NUM_LLM_CALLS:
		metric = domain.WorkflowMetricNumLLMCalls
	case boundflowv1.WorkflowMetric_WORKFLOW_METRIC_LATENCY:
		metric = domain.WorkflowMetricLatency
	case boundflowv1.WorkflowMetric_WORKFLOW_METRIC_APPROVAL_REJECTIONS:
		metric = domain.WorkflowMetricApprovalRejections
	case boundflowv1.WorkflowMetric_WORKFLOW_METRIC_TOOL_FAILURE_RATE:
		metric = domain.WorkflowMetricToolFailureRate
	default:
		metric = domain.WorkflowMetricNumFailures
	}

	var action domain.WorkflowLifecyclePolicyAction
	if r.Action != nil {
		switch r.Action.Type {
		case boundflowv1.WorkflowPolicyActionType_WORKFLOW_POLICY_ACTION_COOLDOWN:
			action = domain.WorkflowLifecyclePolicyAction{
				Type:            domain.WorkflowPolicyActionCooldown,
				CooldownSeconds: int(r.Action.CooldownSeconds),
			}
		case boundflowv1.WorkflowPolicyActionType_WORKFLOW_POLICY_ACTION_SET_VERSION:
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

// WorkflowRuleToProto is the inverse of workflowRuleFromProto (used by the policy
// audit read path to echo the rule that fired).
func WorkflowRuleToProto(r domain.WorkflowLifecyclePolicyRule) *boundflowv1.WorkflowLifecyclePolicyRule {
	var metric boundflowv1.WorkflowMetric
	switch r.Metric {
	case domain.WorkflowMetricCost:
		metric = boundflowv1.WorkflowMetric_WORKFLOW_METRIC_COST
	case domain.WorkflowMetricNumLLMCalls:
		metric = boundflowv1.WorkflowMetric_WORKFLOW_METRIC_NUM_LLM_CALLS
	case domain.WorkflowMetricLatency:
		metric = boundflowv1.WorkflowMetric_WORKFLOW_METRIC_LATENCY
	case domain.WorkflowMetricApprovalRejections:
		metric = boundflowv1.WorkflowMetric_WORKFLOW_METRIC_APPROVAL_REJECTIONS
	case domain.WorkflowMetricToolFailureRate:
		metric = boundflowv1.WorkflowMetric_WORKFLOW_METRIC_TOOL_FAILURE_RATE
	default:
		metric = boundflowv1.WorkflowMetric_WORKFLOW_METRIC_NUM_FAILURES
	}

	var action boundflowv1.WorkflowPolicyActionType
	switch r.Action.Type {
	case domain.WorkflowPolicyActionCooldown:
		action = boundflowv1.WorkflowPolicyActionType_WORKFLOW_POLICY_ACTION_COOLDOWN
	case domain.WorkflowPolicyActionSetVersion:
		action = boundflowv1.WorkflowPolicyActionType_WORKFLOW_POLICY_ACTION_SET_VERSION
	default:
		action = boundflowv1.WorkflowPolicyActionType_WORKFLOW_POLICY_ACTION_PAUSE
	}

	return &boundflowv1.WorkflowLifecyclePolicyRule{
		Metric:    metric,
		Threshold: r.Threshold,
		Window:    int32(r.Window),
		ToolName:  r.ToolName,
		Action: &boundflowv1.WorkflowLifecyclePolicyAction{
			Type:            action,
			CooldownSeconds: int32(r.Action.CooldownSeconds),
			TargetVersion:   int32(r.Action.TargetVersion),
		},
	}
}

func WorkflowConfigFromProto(p *boundflowv1.WorkflowConfig) domain.WorkflowConfig {
	if p == nil {
		return domain.WorkflowConfig{Triggerable: true}
	}
	return domain.WorkflowConfig{
		InvokeTimeoutSeconds: p.InvokeTimeoutSeconds,
		RepeatEverySeconds:   p.RepeatEverySeconds,
		Triggerable:          p.Triggerable,
		InvokeMode:           invokeModeFromProto(p.InvokeMode),
		MaxQueueDepth:        p.MaxQueueDepth,
	}
}

func invokeModeFromProto(m boundflowv1.InvokeMode) domain.InvokeMode {
	if m == boundflowv1.InvokeMode_INVOKE_MODE_QUEUE {
		return domain.InvokeModeQueue
	}
	return domain.InvokeModeCoalesce // UNSPECIFIED and COALESCE both default here
}

func invokeModeToProto(m domain.InvokeMode) boundflowv1.InvokeMode {
	if m == domain.InvokeModeQueue {
		return boundflowv1.InvokeMode_INVOKE_MODE_QUEUE
	}
	return boundflowv1.InvokeMode_INVOKE_MODE_COALESCE
}
