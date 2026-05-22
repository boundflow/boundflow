using System.Text.Json.Nodes;

namespace BoundFlow.SDK.Llm;

/// <summary>
/// Pure functions for evaluating lifecycle rules and mutating runtime policies.
/// Extracted from OperationContext for testability.
/// </summary>
internal static class LifecyclePolicyEvaluator
{
    internal static AgentRuntimePolicy LoadRuntimePolicy(JsonNode? stateNode)
    {
        if (stateNode?["runtime_policy"] is not JsonObject rp) return new AgentRuntimePolicy();
        return new AgentRuntimePolicy(
            MaxLlmCalls:      rp["max_llm_calls"]?.GetValue<int>()              ?? 0,
            MaxCostUsd:       (decimal)(rp["max_cost_usd"]?.GetValue<double>()  ?? 0),
            MaxTokensPerCall: rp["max_tokens_per_call"]?.GetValue<int>()        ?? 0,
            MaxCallsPerTool:  rp["max_calls_per_tool"]?.GetValue<int>()         ?? 0
        );
    }

    internal static AgentLifecyclePolicy? LoadLifecyclePolicy(JsonNode? stateNode)
    {
        if (stateNode?["lifecycle_policy"]?["rules"] is not JsonArray rulesNode) return null;
        var rules = rulesNode
            .OfType<JsonObject>()
            .Select(r => new AgentLifecycleRule(
                Metric:    System.Enum.Parse<AgentMetric>(r["metric"]?.GetValue<string>()   ?? "TokensUsed", ignoreCase: true),
                Operator:  System.Enum.Parse<PolicyOperator>(r["operator"]?.GetValue<string>() ?? "GreaterThan", ignoreCase: true),
                Threshold: (decimal)(r["threshold"]?.GetValue<double>()                     ?? 0),
                Window:    r["window"]?.GetValue<int>()                                     ?? 0,
                Action: new PolicyMutation(
                    System.Enum.Parse<PolicyField>(r["action"]?["field"]?.GetValue<string>() ?? "Model", ignoreCase: true),
                    r["action"]?["value"]?.ToString() ?? ""
                )
            ))
            .ToList();
        return new AgentLifecyclePolicy(rules);
    }

    internal static List<JsonNode> LoadMetricsHistory(JsonNode? stateNode)
    {
        if (stateNode?["invocation_metrics"] is not JsonArray arr) return [];
        return [.. arr.OfType<JsonNode>()];
    }

    internal static AgentRuntimePolicy ApplyLifecycleRules(
        AgentLifecyclePolicy? policy,
        List<JsonNode> history,
        AgentRuntimePolicy current)
    {
        if (policy is null || policy.Rules.Count == 0) return current;

        foreach (var rule in policy.Rules)
        {
            var window = rule.Window > 0 ? history.TakeLast(rule.Window).ToList() : history;
            var sum = window.Sum(e => GetMetricValue(e, rule.Metric));
            if (!Evaluate(sum, rule.Operator, rule.Threshold)) continue;

            current = ApplyMutation(current, rule.Action);
        }
        return current;
    }

    internal static decimal GetMetricValue(JsonNode entry, AgentMetric metric)
    {
        var key = metric switch
        {
            AgentMetric.TokensUsed   => "tokens_used",
            AgentMetric.CostUsd      => "cost_usd",
            AgentMetric.LlmCalls     => "llm_calls",
            AgentMetric.CallsPerTool => "calls_per_tool",
            _                        => null,
        };
        return key is null ? 0 : ToDecimal(entry[key]);
    }

    private static decimal ToDecimal(JsonNode? node)
    {
        if (node is null) return 0;
        var v = node.AsValue();
        if (v.TryGetValue<double>(out var d)) return (decimal)d;
        if (v.TryGetValue<long>(out var l))   return l;
        if (v.TryGetValue<int>(out var i))    return i;
        return 0;
    }

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
            PolicyField.MaxLlmCalls      => policy with { MaxLlmCalls      = int.TryParse(val, out var i)     ? i : policy.MaxLlmCalls },
            PolicyField.MaxCostUsd       => policy with { MaxCostUsd       = decimal.TryParse(val, out var d) ? d : policy.MaxCostUsd },
            PolicyField.MaxTokensPerCall => policy with { MaxTokensPerCall = int.TryParse(val, out var t)     ? t : policy.MaxTokensPerCall },
            PolicyField.MaxCallsPerTool  => policy with { MaxCallsPerTool  = int.TryParse(val, out var c)     ? c : policy.MaxCallsPerTool },
            _                            => policy,
        };
    }
}
