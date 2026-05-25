package domain

type WorkflowMetric string

const (
	WorkflowMetricNumFailures        WorkflowMetric = "num_failures"
	WorkflowMetricCost               WorkflowMetric = "cost"
	WorkflowMetricNumLLMCalls        WorkflowMetric = "num_llm_calls"
	WorkflowMetricLatency            WorkflowMetric = "latency"
	WorkflowMetricApprovalRejections WorkflowMetric = "approval_rejections"
	WorkflowMetricToolFailureRate    WorkflowMetric = "tool_failure_rate"
)

type WorkflowPolicyActionType string

const (
	WorkflowPolicyActionPause      WorkflowPolicyActionType = "pause"
	WorkflowPolicyActionCooldown   WorkflowPolicyActionType = "cooldown"
	WorkflowPolicyActionSetVersion WorkflowPolicyActionType = "set_version"
)

type WorkflowLifecyclePolicyAction struct {
	Type            WorkflowPolicyActionType
	CooldownSeconds int
	TargetVersion   int
}

type WorkflowLifecyclePolicyRule struct {
	Metric    WorkflowMetric
	Threshold float64
	// Window is the number of recent runs to evaluate. 0 = version-total (set_version actions only).
	Window   int
	ToolName string // only for WorkflowMetricToolFailureRate
	Action   WorkflowLifecyclePolicyAction
}

type WorkflowLifecyclePolicy struct {
	Rules []WorkflowLifecyclePolicyRule
}
