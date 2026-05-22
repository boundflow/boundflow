using System.Text.Json.Nodes;

namespace BoundFlow.ControlPlane;

public record TenantGroup(
    string Id,
    string Name
);

public record Tenant(
    string Id,
    string Name,
    string TenantGroupId
);

public record ResourceInstance(
    string Id,
    string TenantId,
    JsonNode? GoalState
);

public enum LifecycleState
{
    Unknown,
    Creating,
    Active,
    Reconciling,
    Deleting,
    Deleted,
    Failed,
}

public record ResourceState(
    JsonNode? CurrentConfigState,
    JsonNode? GoalConfigState,
    LifecycleState LifecycleState
);

public record AgentRuntimePolicy(
    int MaxLlmCalls = 0,
    decimal MaxCostUsd = 0,
    int MaxTokensPerCall = 0,
    int MaxCallsPerTool = 0,
    string? Model = null
);

public enum AgentMetric     { TokensUsed, CostUsd, LlmCalls, CallsPerTool }
public enum PolicyOperator  { LessThan, LessThanOrEqual, GreaterThan, GreaterThanOrEqual, Equal }
public enum PolicyField     { Model, MaxLlmCalls, MaxCostUsd, MaxTokensPerCall, MaxCallsPerTool }

public record PolicyMutation(PolicyField Field, string Value);

public record AgentLifecycleRule(
    AgentMetric Metric,
    PolicyOperator Operator,
    decimal Threshold,
    int Window,
    PolicyMutation Action
);

public record AgentLifecyclePolicy(IReadOnlyList<AgentLifecycleRule> Rules);
