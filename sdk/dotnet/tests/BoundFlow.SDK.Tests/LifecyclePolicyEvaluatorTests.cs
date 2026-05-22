using System.Text.Json.Nodes;
using BoundFlow.SDK.Llm;
using Xunit;

namespace BoundFlow.SDK.Tests;

public class LifecyclePolicyEvaluatorTests
{
    // ── Helpers ──────────────────────────────────────────────────────────────

    private static JsonNode Snapshot(int tokens = 0, double cost = 0, int llmCalls = 0, int callsPerTool = 0) =>
        new JsonObject
        {
            ["tokens_used"]    = tokens,
            ["cost_usd"]       = cost,
            ["llm_calls"]      = llmCalls,
            ["calls_per_tool"] = callsPerTool,
        };

    private static AgentLifecycleRule Rule(
        AgentMetric metric,
        PolicyOperator op,
        decimal threshold,
        int window,
        PolicyField field,
        string value) =>
        new(metric, op, threshold, window, new PolicyMutation(field, value));

    // ── GetMetricValue ───────────────────────────────────────────────────────

    [Fact]
    public void GetMetricValue_TokensUsed_ReturnsCorrectValue()
    {
        var snap = Snapshot(tokens: 1500);
        Assert.Equal(1500m, LifecyclePolicyEvaluator.GetMetricValue(snap, AgentMetric.TokensUsed));
    }

    [Fact]
    public void GetMetricValue_CostUsd_ReturnsCorrectValue()
    {
        var snap = Snapshot(cost: 0.05);
        Assert.Equal(0.05m, LifecyclePolicyEvaluator.GetMetricValue(snap, AgentMetric.CostUsd));
    }

    [Fact]
    public void GetMetricValue_LlmCalls_ReturnsCorrectValue()
    {
        var snap = Snapshot(llmCalls: 3);
        Assert.Equal(3m, LifecyclePolicyEvaluator.GetMetricValue(snap, AgentMetric.LlmCalls));
    }

    [Fact]
    public void GetMetricValue_CallsPerTool_ReturnsCorrectValue()
    {
        var snap = Snapshot(callsPerTool: 7);
        Assert.Equal(7m, LifecyclePolicyEvaluator.GetMetricValue(snap, AgentMetric.CallsPerTool));
    }

    [Fact]
    public void GetMetricValue_MissingField_ReturnsZero()
    {
        var snap = new JsonObject(); // no keys
        Assert.Equal(0m, LifecyclePolicyEvaluator.GetMetricValue(snap, AgentMetric.TokensUsed));
    }

    // ── Evaluate ─────────────────────────────────────────────────────────────

    [Theory]
    [InlineData(4, 5, true)]   // 4 < 5
    [InlineData(5, 5, false)]  // 5 is not < 5
    public void Evaluate_LessThan(decimal sum, decimal threshold, bool expected) =>
        Assert.Equal(expected, LifecyclePolicyEvaluator.Evaluate(sum, PolicyOperator.LessThan, threshold));

    [Theory]
    [InlineData(5, 5, true)]   // 5 <= 5
    [InlineData(6, 5, false)]  // 6 is not <= 5
    public void Evaluate_LessThanOrEqual(decimal sum, decimal threshold, bool expected) =>
        Assert.Equal(expected, LifecyclePolicyEvaluator.Evaluate(sum, PolicyOperator.LessThanOrEqual, threshold));

    [Theory]
    [InlineData(6, 5, true)]   // 6 > 5
    [InlineData(5, 5, false)]  // 5 is not > 5
    public void Evaluate_GreaterThan(decimal sum, decimal threshold, bool expected) =>
        Assert.Equal(expected, LifecyclePolicyEvaluator.Evaluate(sum, PolicyOperator.GreaterThan, threshold));

    [Theory]
    [InlineData(5, 5, true)]   // 5 >= 5
    [InlineData(4, 5, false)]  // 4 is not >= 5
    public void Evaluate_GreaterThanOrEqual(decimal sum, decimal threshold, bool expected) =>
        Assert.Equal(expected, LifecyclePolicyEvaluator.Evaluate(sum, PolicyOperator.GreaterThanOrEqual, threshold));

    [Theory]
    [InlineData(5, 5, true)]
    [InlineData(4, 5, false)]
    public void Evaluate_Equal(decimal sum, decimal threshold, bool expected) =>
        Assert.Equal(expected, LifecyclePolicyEvaluator.Evaluate(sum, PolicyOperator.Equal, threshold));

    // ── ApplyMutation ────────────────────────────────────────────────────────

    [Fact]
    public void ApplyMutation_MaxLlmCalls_UpdatesField()
    {
        var policy = new AgentRuntimePolicy();
        var result = LifecyclePolicyEvaluator.ApplyMutation(policy, new PolicyMutation(PolicyField.MaxLlmCalls, "5"));
        Assert.Equal(5, result.MaxLlmCalls);
    }

    [Fact]
    public void ApplyMutation_MaxCostUsd_UpdatesField()
    {
        var policy = new AgentRuntimePolicy();
        var result = LifecyclePolicyEvaluator.ApplyMutation(policy, new PolicyMutation(PolicyField.MaxCostUsd, "0.10"));
        Assert.Equal(0.10m, result.MaxCostUsd);
    }

    [Fact]
    public void ApplyMutation_MaxTokensPerCall_UpdatesField()
    {
        var policy = new AgentRuntimePolicy();
        var result = LifecyclePolicyEvaluator.ApplyMutation(policy, new PolicyMutation(PolicyField.MaxTokensPerCall, "2048"));
        Assert.Equal(2048, result.MaxTokensPerCall);
    }

    [Fact]
    public void ApplyMutation_MaxCallsPerTool_UpdatesField()
    {
        var policy = new AgentRuntimePolicy();
        var result = LifecyclePolicyEvaluator.ApplyMutation(policy, new PolicyMutation(PolicyField.MaxCallsPerTool, "3"));
        Assert.Equal(3, result.MaxCallsPerTool);
    }

    [Fact]
    public void ApplyMutation_InvalidValue_LeavesFieldUnchanged()
    {
        var policy = new AgentRuntimePolicy(MaxLlmCalls: 10);
        var result = LifecyclePolicyEvaluator.ApplyMutation(policy, new PolicyMutation(PolicyField.MaxLlmCalls, "not-a-number"));
        Assert.Equal(10, result.MaxLlmCalls);
    }

    [Fact]
    public void ApplyMutation_OtherFieldsUnchanged()
    {
        var policy = new AgentRuntimePolicy(MaxLlmCalls: 5, MaxCostUsd: 1.0m);
        var result = LifecyclePolicyEvaluator.ApplyMutation(policy, new PolicyMutation(PolicyField.MaxLlmCalls, "10"));
        Assert.Equal(1.0m, result.MaxCostUsd); // unchanged
    }

    // ── ApplyLifecycleRules ──────────────────────────────────────────────────

    [Fact]
    public void ApplyLifecycleRules_NullPolicy_ReturnsPolicyUnchanged()
    {
        var policy = new AgentRuntimePolicy(MaxLlmCalls: 5);
        var result = LifecyclePolicyEvaluator.ApplyLifecycleRules(null, [], policy);
        Assert.Equal(5, result.MaxLlmCalls);
    }

    [Fact]
    public void ApplyLifecycleRules_EmptyRules_ReturnsPolicyUnchanged()
    {
        var policy = new AgentRuntimePolicy(MaxLlmCalls: 5);
        var result = LifecyclePolicyEvaluator.ApplyLifecycleRules(new AgentLifecyclePolicy([]), [], policy);
        Assert.Equal(5, result.MaxLlmCalls);
    }

    [Fact]
    public void ApplyLifecycleRules_EmptyHistory_RuleDoesNotFire()
    {
        // sum of tokens over empty history = 0, threshold = 1000, so rule should NOT fire
        var lifecyclePolicy = new AgentLifecyclePolicy([
            Rule(AgentMetric.TokensUsed, PolicyOperator.GreaterThan, 1000, window: 10,
                 PolicyField.MaxLlmCalls, "3")
        ]);
        var policy = new AgentRuntimePolicy();
        var result = LifecyclePolicyEvaluator.ApplyLifecycleRules(lifecyclePolicy, [], policy);
        Assert.Equal(0, result.MaxLlmCalls); // unchanged default
    }

    [Fact]
    public void ApplyLifecycleRules_ThresholdExceeded_MutatesPolicyField()
    {
        var history = new List<JsonNode>
        {
            Snapshot(tokens: 500),
            Snapshot(tokens: 600), // sum = 1100 > 1000
        };
        var lifecyclePolicy = new AgentLifecyclePolicy([
            Rule(AgentMetric.TokensUsed, PolicyOperator.GreaterThan, 1000, window: 10,
                 PolicyField.MaxLlmCalls, "3")
        ]);
        var result = LifecyclePolicyEvaluator.ApplyLifecycleRules(lifecyclePolicy, history, new AgentRuntimePolicy());
        Assert.Equal(3, result.MaxLlmCalls);
    }

    [Fact]
    public void ApplyLifecycleRules_ThresholdNotExceeded_PolicyUnchanged()
    {
        var history = new List<JsonNode>
        {
            Snapshot(tokens: 300),
            Snapshot(tokens: 400), // sum = 700 < 1000
        };
        var lifecyclePolicy = new AgentLifecyclePolicy([
            Rule(AgentMetric.TokensUsed, PolicyOperator.GreaterThan, 1000, window: 10,
                 PolicyField.MaxLlmCalls, "3")
        ]);
        var result = LifecyclePolicyEvaluator.ApplyLifecycleRules(lifecyclePolicy, history, new AgentRuntimePolicy());
        Assert.Equal(0, result.MaxLlmCalls); // default unchanged
    }

    [Fact]
    public void ApplyLifecycleRules_Window_OnlyLastNInvocationsConsidered()
    {
        // 5 snapshots, window = 3: only last 3 count
        // last 3: 200 + 200 + 200 = 600, which is NOT > 1000
        // all 5:  400 + 400 + 200 + 200 + 200 = 1400, which IS > 1000
        var history = new List<JsonNode>
        {
            Snapshot(tokens: 400),
            Snapshot(tokens: 400),
            Snapshot(tokens: 200),
            Snapshot(tokens: 200),
            Snapshot(tokens: 200),
        };
        var lifecyclePolicy = new AgentLifecyclePolicy([
            Rule(AgentMetric.TokensUsed, PolicyOperator.GreaterThan, 1000, window: 3,
                 PolicyField.MaxLlmCalls, "3")
        ]);
        var result = LifecyclePolicyEvaluator.ApplyLifecycleRules(lifecyclePolicy, history, new AgentRuntimePolicy());
        Assert.Equal(0, result.MaxLlmCalls); // rule did not fire — window kept it below threshold
    }

    [Fact]
    public void ApplyLifecycleRules_Window_FiresWhenWindowSumExceedsThreshold()
    {
        // window = 3, last 3: 400 + 400 + 400 = 1200 > 1000
        var history = new List<JsonNode>
        {
            Snapshot(tokens: 100),
            Snapshot(tokens: 400),
            Snapshot(tokens: 400),
            Snapshot(tokens: 400),
        };
        var lifecyclePolicy = new AgentLifecyclePolicy([
            Rule(AgentMetric.TokensUsed, PolicyOperator.GreaterThan, 1000, window: 3,
                 PolicyField.MaxLlmCalls, "3")
        ]);
        var result = LifecyclePolicyEvaluator.ApplyLifecycleRules(lifecyclePolicy, history, new AgentRuntimePolicy());
        Assert.Equal(3, result.MaxLlmCalls);
    }

    [Fact]
    public void ApplyLifecycleRules_MultipleRules_AllFiringRulesApplied()
    {
        var history = new List<JsonNode>
        {
            Snapshot(tokens: 2000, llmCalls: 10),
        };
        var lifecyclePolicy = new AgentLifecyclePolicy([
            Rule(AgentMetric.TokensUsed, PolicyOperator.GreaterThan, 1000, window: 5,
                 PolicyField.MaxLlmCalls, "3"),
            Rule(AgentMetric.LlmCalls,   PolicyOperator.GreaterThan, 5,    window: 5,
                 PolicyField.MaxCostUsd,  "0.50"),
        ]);
        var result = LifecyclePolicyEvaluator.ApplyLifecycleRules(lifecyclePolicy, history, new AgentRuntimePolicy());
        Assert.Equal(3,     result.MaxLlmCalls);
        Assert.Equal(0.50m, result.MaxCostUsd);
    }

    [Fact]
    public void ApplyLifecycleRules_MultipleRules_OnlyFiringRulesApplied()
    {
        var history = new List<JsonNode>
        {
            Snapshot(tokens: 2000, llmCalls: 2), // tokens fires, llmCalls does not
        };
        var lifecyclePolicy = new AgentLifecyclePolicy([
            Rule(AgentMetric.TokensUsed, PolicyOperator.GreaterThan, 1000, window: 5,
                 PolicyField.MaxLlmCalls, "3"),
            Rule(AgentMetric.LlmCalls,   PolicyOperator.GreaterThan, 5,    window: 5,
                 PolicyField.MaxCostUsd,  "0.50"),
        ]);
        var result = LifecyclePolicyEvaluator.ApplyLifecycleRules(lifecyclePolicy, history, new AgentRuntimePolicy());
        Assert.Equal(3,  result.MaxLlmCalls);
        Assert.Equal(0m, result.MaxCostUsd); // second rule did not fire
    }

    // ── LoadRuntimePolicy ────────────────────────────────────────────────────

    [Fact]
    public void LoadRuntimePolicy_NullNode_ReturnsDefaults()
    {
        var policy = LifecyclePolicyEvaluator.LoadRuntimePolicy(null);
        Assert.Equal(new AgentRuntimePolicy(), policy);
    }

    [Fact]
    public void LoadRuntimePolicy_PopulatedJson_ParsesAllFields()
    {
        var stateNode = JsonNode.Parse("""
            {
              "runtime_policy": {
                "max_llm_calls": 5,
                "max_cost_usd": 0.25,
                "max_tokens_per_call": 2048,
                "max_calls_per_tool": 3
              }
            }
            """);
        var policy = LifecyclePolicyEvaluator.LoadRuntimePolicy(stateNode);
        Assert.Equal(5,      policy.MaxLlmCalls);
        Assert.Equal(0.25m,  policy.MaxCostUsd);
        Assert.Equal(2048,   policy.MaxTokensPerCall);
        Assert.Equal(3,      policy.MaxCallsPerTool);
    }

    // ── LoadLifecyclePolicy ──────────────────────────────────────────────────

    [Fact]
    public void LoadLifecyclePolicy_NullNode_ReturnsNull()
    {
        Assert.Null(LifecyclePolicyEvaluator.LoadLifecyclePolicy(null));
    }

    [Fact]
    public void LoadLifecyclePolicy_EmptyRules_ReturnsEmptyPolicy()
    {
        var stateNode = JsonNode.Parse("""{"lifecycle_policy": {"rules": []}}""");
        var policy = LifecyclePolicyEvaluator.LoadLifecyclePolicy(stateNode);
        Assert.NotNull(policy);
        Assert.Empty(policy.Rules);
    }

    [Fact]
    public void LoadLifecyclePolicy_PopulatedJson_ParsesRule()
    {
        var stateNode = JsonNode.Parse("""
            {
              "lifecycle_policy": {
                "rules": [{
                  "metric": "TokensUsed",
                  "operator": "GreaterThan",
                  "threshold": 1000,
                  "window": 10,
                  "action": { "field": "MaxLlmCalls", "value": "3" }
                }]
              }
            }
            """);
        var policy = LifecyclePolicyEvaluator.LoadLifecyclePolicy(stateNode);
        Assert.NotNull(policy);
        Assert.Single(policy.Rules);
        var rule = policy.Rules[0];
        Assert.Equal(AgentMetric.TokensUsed,       rule.Metric);
        Assert.Equal(PolicyOperator.GreaterThan,   rule.Operator);
        Assert.Equal(1000m,                        rule.Threshold);
        Assert.Equal(10,                           rule.Window);
        Assert.Equal(PolicyField.MaxLlmCalls,      rule.Action.Field);
        Assert.Equal("3",                          rule.Action.Value?.ToString());
    }
}
