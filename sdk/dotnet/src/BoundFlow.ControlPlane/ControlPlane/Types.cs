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
    int Version = 0,
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


public record RuntimeOverrides(
    int OperationTimeoutSeconds = 0
);

public record AgentRuntimePolicy(
    int MaxLlmCalls = 0,
    decimal MaxCostUsd = 0,
    int MaxTokensPerCall = 0,
    int MaxCallsPerTool = 0,
    string? Model = null
);

public enum WorkflowState { Unspecified, Active, Paused, Cooldown, Disabled, Deleted }

public enum WorkflowMetric
{
    NumFailures,
    Cost,
    NumLlmCalls,
    Latency,
    ApprovalRejections,
    ToolFailureRate,
}

public abstract record WorkflowLifecyclePolicyAction;
public record PauseAction(int Window) : WorkflowLifecyclePolicyAction;
public record CooldownAction(int Window, int CooldownSeconds) : WorkflowLifecyclePolicyAction;
public record SetVersionAction(int TargetVersion) : WorkflowLifecyclePolicyAction;

public record WorkflowLifecyclePolicyRule(
    WorkflowMetric Metric,
    double Threshold,
    WorkflowLifecyclePolicyAction Action,
    string? ToolName = null
);

public record WorkflowLifecyclePolicy(IReadOnlyList<WorkflowLifecyclePolicyRule> Rules);

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
