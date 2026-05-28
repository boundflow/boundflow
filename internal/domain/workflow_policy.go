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
	Type            WorkflowPolicyActionType `json:"type"`
	CooldownSeconds int                      `json:"cooldown_seconds"`
	TargetVersion   int                      `json:"target_version"`
}

type WorkflowLifecyclePolicyRule struct {
	Metric    WorkflowMetric                `json:"metric"`
	Threshold float64                       `json:"threshold"`
	// Window is the number of recent runs to evaluate. 0 = version-total (set_version actions only).
	Window   int                            `json:"window"`
	ToolName string                         `json:"tool_name,omitempty"`
	Action   WorkflowLifecyclePolicyAction  `json:"action"`
}

type WorkflowLifecyclePolicy struct {
	Rules []WorkflowLifecyclePolicyRule `json:"rules"`
}

type WorkflowInvocationSnapshot struct {
	CostUsd            *float64       `json:"cost_usd,omitempty"`
	LlmCalls           *int           `json:"llm_calls,omitempty"`
	LatencySeconds     *float64       `json:"latency_seconds,omitempty"`
	Failures           *int           `json:"failures,omitempty"`
	ApprovalRejections *int           `json:"approval_rejections,omitempty"`
	ToolFailureCounts  map[string]int `json:"tool_failure_counts,omitempty"`
	RanAt              int64          `json:"ran_at"`
	LastMeasured       int64          `json:"last_measured"`
}

type WorkflowVersionMetrics struct {
	ResourceInstanceID      string
	Version                 int
	Epoch                   int
	TotalCost               float64
	RunCount                int
	TotalFailures           int
	TotalLLMCalls           int
	TotalLatencySeconds     float64
	TotalApprovalRejections int
	ToolFailureCounts       map[string]int
	LastMeasured            int64
}
