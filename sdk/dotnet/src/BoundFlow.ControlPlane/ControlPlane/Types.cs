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

public record Workflow(
    string Id,
    string TenantId,
    WorkflowConfig Config
);
public record WorkflowConfig(
    int InitialVersion = 0,
    int InvokeTimeoutSeconds = 60,
    int RepeatEverySeconds = 0,
    bool Triggerable = true
);

public enum LifecycleState
{
    Unknown,
    Creating,
    Active,
    Invoking,
    Deleting,
    Deleted,
    Failed,
}

public record WorkflowState(
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
