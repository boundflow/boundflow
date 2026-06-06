using System.Text.Json;
using System.Text.Json.Nodes;

namespace BoundFlow.SDK.Llm;

/// <summary>
/// Pure functions for evaluating lifecycle rules and mutating runtime policies.
/// Extracted from OperationContext for testability.
/// </summary>
internal static class LifecyclePolicyEvaluator
{
    private static readonly JsonSerializerOptions SnakeCase = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower,
    };

    internal static AgentRuntimePolicy LoadRuntimePolicy(JsonNode? policyNode)
    {
        if (policyNode is not JsonObject rp) return new AgentRuntimePolicy();
        List<ToolCallLimit>? toolCallLimits = null;
        if (rp["tool_call_limits"] is JsonArray limits)
        {
            toolCallLimits = limits
                .OfType<JsonObject>()
                .Select(l => new ToolCallLimit(
                    l["tool_name"]?.GetValue<string>() ?? "",
                    l["max_calls"]?.GetValue<int>()    ?? 0))
                .Where(l => l.ToolName.Length > 0)
                .ToList();
        }
        return new AgentRuntimePolicy(
            MaxLlmCalls:      rp["max_llm_calls"]?.GetValue<int>()             ?? 0,
            MaxCostUsd:       (decimal)(rp["max_cost_usd"]?.GetValue<double>() ?? 0),
            MaxTokensPerCall: rp["max_tokens_per_call"]?.GetValue<int>()       ?? 0,
            ToolCallLimits:   toolCallLimits,
            Model:            rp["model"]?.GetValue<string>()
        );
    }

    internal static AgentLifecyclePolicy? LoadLifecyclePolicy(JsonNode? stateNode)
    {
        if (stateNode?["lifecycle_policy"]?["rules"] is not JsonArray rulesNode) return null;
        var rules = rulesNode
            .OfType<JsonObject>()
            .Select(r => new AgentLifecycleRule(
                Metric:    System.Enum.Parse<AgentMetric>(r["metric"]?.GetValue<string>()      ?? "TokensUsed",   ignoreCase: true),
                Operator:  System.Enum.Parse<PolicyOperator>(r["operator"]?.GetValue<string>() ?? "GreaterThan",  ignoreCase: true),
                Threshold: (decimal)(r["threshold"]?.GetValue<double>()                        ?? 0),
                Window:    r["window"]?.GetValue<int>()                                        ?? 0,
                Action:    new PolicyMutation(
                    System.Enum.Parse<PolicyField>(r["action"]?["field"]?.GetValue<string>()   ?? "Model", ignoreCase: true),
                    r["action"]?["value"]?.ToString() ?? ""
                ),
                ToolName:  r["tool_name"]?.GetValue<string>()
            ))
            .ToList();
        return new AgentLifecyclePolicy(rules);
    }

    internal static List<InvocationSnapshot> LoadMetricsHistory(JsonNode? stateNode)
    {
        if (stateNode?["invocation_metrics"] is not JsonArray arr) return [];
        return JsonSerializer.Deserialize<List<InvocationSnapshot>>(arr.ToJsonString(), SnakeCase) ?? [];
    }

    internal static JsonNode SerializeMetricsHistory(List<InvocationSnapshot> history) =>
        JsonSerializer.SerializeToNode(history, SnakeCase)!;

    internal static AgentRuntimePolicy ApplyLifecycleRules(
        AgentLifecyclePolicy? policy,
        List<InvocationSnapshot> history,
        AgentRuntimePolicy current)
    {
        if (policy is null || policy.Rules.Count == 0) return current;

        foreach (var rule in policy.Rules)
        {
            var window = rule.Window > 0 ? history.TakeLast(rule.Window).ToList() : history;
            var sum = window.Sum(e => GetMetricValue(e, rule.Metric, rule.ToolName));
            if (!Evaluate(sum, rule.Operator, rule.Threshold)) continue;

            current = ApplyMutation(current, rule.Action);
        }
        return current;
    }

    internal static decimal GetMetricValue(InvocationSnapshot entry, AgentMetric metric, string? toolName = null) => metric switch
    {
        AgentMetric.TokensUsed   => entry.TokensUsed,
        AgentMetric.CostUsd      => (decimal)entry.CostUsd,
        AgentMetric.LlmCalls     => entry.LlmCalls,
        AgentMetric.CallsPerTool => toolName is not null
            ? entry.CallsPerTool?.GetValueOrDefault(toolName) ?? 0
            : entry.CallsPerTool?.Values.DefaultIfEmpty(0).Max() ?? 0,
        _                        => 0,
    };

    internal static bool Evaluate(decimal sum, PolicyOperator op, decimal threshold) => op switch
    {
        PolicyOperator.LessThan           => sum < threshold,
        PolicyOperator.LessThanOrEqual    => sum <= threshold,
        PolicyOperator.GreaterThan        => sum > threshold,
        PolicyOperator.GreaterThanOrEqual => sum >= threshold,
        PolicyOperator.Equal              => sum == threshold,
        _                                 => false,
    };

    internal static AgentRuntimePolicy ApplyMutation(AgentRuntimePolicy policy, PolicyMutation mutation)
    {
        var val = mutation.Value?.ToString() ?? "";
        return mutation.Field switch
        {
            PolicyField.Model            => policy with { Model            = string.IsNullOrEmpty(val)         ? policy.Model : val },
            PolicyField.MaxLlmCalls      => policy with { MaxLlmCalls      = int.TryParse(val, out var i)     ? i : policy.MaxLlmCalls },
            PolicyField.MaxCostUsd       => policy with { MaxCostUsd       = decimal.TryParse(val, out var d) ? d : policy.MaxCostUsd },
            PolicyField.MaxTokensPerCall => policy with { MaxTokensPerCall = int.TryParse(val, out var t)     ? t : policy.MaxTokensPerCall },
            _                            => policy,
        };
    }
}
