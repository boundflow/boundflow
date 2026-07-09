package domain

// Typed agent-policy primitives. Currently used by the agent-lifecycle audit
// (AgentPolicyActionDetails); they mirror the SDK policy types and are reusable if
// the agent-policy RPCs ever move off opaque JSON onto these.

type AgentMetric string
type AgentOp string

type ToolCallLimit struct {
	Tool     string `json:"tool"`
	MaxCalls int    `json:"max_calls"`
}

// AgentRuntimePolicy mirrors the SDK RuntimePolicy (hard caps + model override).
type AgentRuntimePolicy struct {
	Model            string          `json:"model,omitempty"`
	MaxLlmCalls      int             `json:"max_llm_calls"`
	MaxCostUsd       float64         `json:"max_cost_usd"`
	MaxTokensPerCall int             `json:"max_tokens_per_call"`
	MaxCallSeconds   float64         `json:"max_call_seconds"`
	ToolCallLimits   []ToolCallLimit `json:"tool_call_limits,omitempty"`
}

// AgentRuleAction is the discriminated action: Field names the changed setting,
// with the value in the matching field.
type AgentRuleAction struct {
	Field            string  `json:"field"` // model | max_llm_calls | max_cost_usd | max_tokens_per_call
	Model            string  `json:"model,omitempty"`
	MaxLlmCalls      int     `json:"max_llm_calls,omitempty"`
	MaxCostUsd       float64 `json:"max_cost_usd,omitempty"`
	MaxTokensPerCall int     `json:"max_tokens_per_call,omitempty"`
}

type AgentRule struct {
	Metric    AgentMetric     `json:"metric"`
	Op        AgentOp         `json:"op"`
	Threshold float64         `json:"threshold"`
	Window    int             `json:"window"`
	Tool      string          `json:"tool,omitempty"`
	Action    AgentRuleAction `json:"action"`
}

type FiredAgentRule struct {
	Rule         AgentRule `json:"rule"`
	TriggerValue float64   `json:"trigger_value"`
}

// AgentPolicyActionDetails is the typed payload of a policy_action audit event with
// scope="agent": the agent whose lifecycle rules changed its effective runtime
// policy this run, the base (pre-run) → effective (post-rules) policies, and the
// rules that fired. The changed variables are base vs effective.
type AgentPolicyActionDetails struct {
	Scope           string             `json:"scope"` // "agent"
	Agent           string             `json:"agent"`
	BasePolicy      AgentRuntimePolicy `json:"base_policy"`
	EffectivePolicy AgentRuntimePolicy `json:"effective_policy"`
	FiredRules      []FiredAgentRule   `json:"fired_rules"`
}
