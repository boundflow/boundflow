package convert

import (
	boundflowv1 "github.com/boundflow/boundflow/gen/boundflow/v1"
	"github.com/boundflow/boundflow/internal/domain"
)

// --- enums ---

func agentMetricFromProto(m boundflowv1.AgentMetric) domain.AgentMetric {
	switch m {
	case boundflowv1.AgentMetric_AGENT_METRIC_TOKENS_USED:
		return "tokens_used"
	case boundflowv1.AgentMetric_AGENT_METRIC_COST_USD:
		return "cost_usd"
	case boundflowv1.AgentMetric_AGENT_METRIC_LLM_CALLS:
		return "llm_calls"
	case boundflowv1.AgentMetric_AGENT_METRIC_CALLS_PER_TOOL:
		return "calls_per_tool"
	}
	return ""
}

func agentMetricToProto(m domain.AgentMetric) boundflowv1.AgentMetric {
	switch m {
	case "tokens_used":
		return boundflowv1.AgentMetric_AGENT_METRIC_TOKENS_USED
	case "cost_usd":
		return boundflowv1.AgentMetric_AGENT_METRIC_COST_USD
	case "llm_calls":
		return boundflowv1.AgentMetric_AGENT_METRIC_LLM_CALLS
	case "calls_per_tool":
		return boundflowv1.AgentMetric_AGENT_METRIC_CALLS_PER_TOOL
	}
	return boundflowv1.AgentMetric_AGENT_METRIC_UNSPECIFIED
}

func agentOpFromProto(o boundflowv1.AgentOp) domain.AgentOp {
	switch o {
	case boundflowv1.AgentOp_AGENT_OP_LT:
		return "less_than"
	case boundflowv1.AgentOp_AGENT_OP_LTE:
		return "less_than_or_equal"
	case boundflowv1.AgentOp_AGENT_OP_GT:
		return "greater_than"
	case boundflowv1.AgentOp_AGENT_OP_GTE:
		return "greater_than_or_equal"
	case boundflowv1.AgentOp_AGENT_OP_EQ:
		return "equal"
	}
	return ""
}

func agentOpToProto(o domain.AgentOp) boundflowv1.AgentOp {
	switch o {
	case "less_than":
		return boundflowv1.AgentOp_AGENT_OP_LT
	case "less_than_or_equal":
		return boundflowv1.AgentOp_AGENT_OP_LTE
	case "greater_than":
		return boundflowv1.AgentOp_AGENT_OP_GT
	case "greater_than_or_equal":
		return boundflowv1.AgentOp_AGENT_OP_GTE
	case "equal":
		return boundflowv1.AgentOp_AGENT_OP_EQ
	}
	return boundflowv1.AgentOp_AGENT_OP_UNSPECIFIED
}

// --- runtime policy ---

func agentRuntimePolicyFromProto(p *boundflowv1.AgentRuntimePolicy) domain.AgentRuntimePolicy {
	if p == nil {
		return domain.AgentRuntimePolicy{}
	}
	limits := make([]domain.ToolCallLimit, 0, len(p.ToolCallLimits))
	for _, l := range p.ToolCallLimits {
		limits = append(limits, domain.ToolCallLimit{Tool: l.Tool, MaxCalls: int(l.MaxCalls)})
	}
	return domain.AgentRuntimePolicy{
		Model:            p.Model,
		MaxLlmCalls:      int(p.MaxLlmCalls),
		MaxCostUsd:       p.MaxCostUsd,
		MaxTokensPerCall: int(p.MaxTokensPerCall),
		ToolCallLimits:   limits,
	}
}

func agentRuntimePolicyToProto(p domain.AgentRuntimePolicy) *boundflowv1.AgentRuntimePolicy {
	limits := make([]*boundflowv1.ToolCallLimit, 0, len(p.ToolCallLimits))
	for _, l := range p.ToolCallLimits {
		limits = append(limits, &boundflowv1.ToolCallLimit{Tool: l.Tool, MaxCalls: int32(l.MaxCalls)})
	}
	return &boundflowv1.AgentRuntimePolicy{
		Model:            p.Model,
		MaxLlmCalls:      int32(p.MaxLlmCalls),
		MaxCostUsd:       p.MaxCostUsd,
		MaxTokensPerCall: int32(p.MaxTokensPerCall),
		ToolCallLimits:   limits,
	}
}

// --- rule action ---

func agentRuleActionFromProto(a *boundflowv1.AgentRuleAction) domain.AgentRuleAction {
	if a == nil {
		return domain.AgentRuleAction{}
	}
	switch a.Field {
	case boundflowv1.AgentRuleActionField_AGENT_RULE_ACTION_SET_MODEL:
		return domain.AgentRuleAction{Field: "model", Model: a.Model}
	case boundflowv1.AgentRuleActionField_AGENT_RULE_ACTION_SET_MAX_LLM_CALLS:
		return domain.AgentRuleAction{Field: "max_llm_calls", MaxLlmCalls: int(a.MaxLlmCalls)}
	case boundflowv1.AgentRuleActionField_AGENT_RULE_ACTION_SET_MAX_COST_USD:
		return domain.AgentRuleAction{Field: "max_cost_usd", MaxCostUsd: a.MaxCostUsd}
	case boundflowv1.AgentRuleActionField_AGENT_RULE_ACTION_SET_MAX_TOKENS_PER_CALL:
		return domain.AgentRuleAction{Field: "max_tokens_per_call", MaxTokensPerCall: int(a.MaxTokensPerCall)}
	}
	return domain.AgentRuleAction{}
}

func agentRuleActionToProto(a domain.AgentRuleAction) *boundflowv1.AgentRuleAction {
	switch a.Field {
	case "model":
		return &boundflowv1.AgentRuleAction{Field: boundflowv1.AgentRuleActionField_AGENT_RULE_ACTION_SET_MODEL, Model: a.Model}
	case "max_llm_calls":
		return &boundflowv1.AgentRuleAction{Field: boundflowv1.AgentRuleActionField_AGENT_RULE_ACTION_SET_MAX_LLM_CALLS, MaxLlmCalls: int32(a.MaxLlmCalls)}
	case "max_cost_usd":
		return &boundflowv1.AgentRuleAction{Field: boundflowv1.AgentRuleActionField_AGENT_RULE_ACTION_SET_MAX_COST_USD, MaxCostUsd: a.MaxCostUsd}
	case "max_tokens_per_call":
		return &boundflowv1.AgentRuleAction{Field: boundflowv1.AgentRuleActionField_AGENT_RULE_ACTION_SET_MAX_TOKENS_PER_CALL, MaxTokensPerCall: int32(a.MaxTokensPerCall)}
	}
	return &boundflowv1.AgentRuleAction{}
}

// --- rule ---

func agentRuleFromProto(r *boundflowv1.AgentRule) domain.AgentRule {
	if r == nil {
		return domain.AgentRule{}
	}
	return domain.AgentRule{
		Metric:    agentMetricFromProto(r.Metric),
		Op:        agentOpFromProto(r.Op),
		Threshold: r.Threshold,
		Window:    int(r.Window),
		Tool:      r.Tool,
		Action:    agentRuleActionFromProto(r.Action),
	}
}

func agentRuleToProto(r domain.AgentRule) *boundflowv1.AgentRule {
	return &boundflowv1.AgentRule{
		Metric:    agentMetricToProto(r.Metric),
		Op:        agentOpToProto(r.Op),
		Threshold: r.Threshold,
		Window:    int32(r.Window),
		Tool:      r.Tool,
		Action:    agentRuleActionToProto(r.Action),
	}
}

// --- top level ---

// AgentPolicyActionFromProto builds the typed audit detail from the reported action
// (the agent name is the map key in the operation result).
func AgentPolicyActionFromProto(agent string, pa *boundflowv1.AgentPolicyAction) domain.AgentPolicyActionDetails {
	fired := make([]domain.FiredAgentRule, 0, len(pa.FiredRules))
	for _, f := range pa.FiredRules {
		fired = append(fired, domain.FiredAgentRule{
			Rule:         agentRuleFromProto(f.Rule),
			TriggerValue: f.TriggerValue,
		})
	}
	return domain.AgentPolicyActionDetails{
		Scope:           "agent",
		Agent:           agent,
		BasePolicy:      agentRuntimePolicyFromProto(pa.BasePolicy),
		EffectivePolicy: agentRuntimePolicyFromProto(pa.EffectivePolicy),
		FiredRules:      fired,
	}
}

// AgentPolicyActionToProto is the inverse, for the audit read path.
func AgentPolicyActionToProto(d domain.AgentPolicyActionDetails) *boundflowv1.AgentPolicyAction {
	fired := make([]*boundflowv1.FiredAgentRule, 0, len(d.FiredRules))
	for _, f := range d.FiredRules {
		fired = append(fired, &boundflowv1.FiredAgentRule{
			Rule:         agentRuleToProto(f.Rule),
			TriggerValue: f.TriggerValue,
		})
	}
	return &boundflowv1.AgentPolicyAction{
		BasePolicy:      agentRuntimePolicyToProto(d.BasePolicy),
		EffectivePolicy: agentRuntimePolicyToProto(d.EffectivePolicy),
		FiredRules:      fired,
	}
}
