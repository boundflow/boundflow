using System.Text.Json.Nodes;

namespace BoundFlow.SDK.Llm;

/// <summary>
/// Per-step runtime limits enforced by the orchestrator.
/// Server-stored values (loaded from agent_state context) take precedence over these.
/// </summary>
internal record InvocationSnapshot(
    int TokensUsed,
    double CostUsd,
    int LlmCalls,
    int CallsPerTool,
    long RanAt
);

internal record AgentRuntimePolicy(
    int MaxLlmCalls = 0,
    decimal MaxCostUsd = 0,
    int MaxTokensPerCall = 0,
    int MaxCallsPerTool = 0,
    IReadOnlyList<string>? AllowedModels = null,
    // Written by lifecycle rules only. Null = use AgentDefinition.Model.
    string? Model = null
);

internal record AgentLifecycleRule(
    AgentMetric Metric,
    PolicyOperator Operator,
    decimal Threshold,
    int Window,
    PolicyMutation Action
);

internal record WorkflowConfig(
    int Version,
    double TimeoutSeconds,
    double RepeatEvery,
    bool Triggerable
);

internal enum AgentMetric     { TokensUsed, CostUsd, LlmCalls, CallsPerTool }
internal enum PolicyOperator  { LessThan, LessThanOrEqual, GreaterThan, GreaterThanOrEqual, Equal }
internal record PolicyMutation(PolicyField Field, object Value);
internal enum PolicyField     { Model, MaxLlmCalls, MaxCostUsd, MaxTokensPerCall, MaxCallsPerTool }
internal record AgentLifecyclePolicy(IReadOnlyList<AgentLifecycleRule> Rules);

/// <summary>
/// Inline definition of an agent step — system prompt, callbacks, and output schema.
/// Policies are loaded from agent_state in the operation context.
/// </summary>
public record AgentDefinition(
    string Name,
    string SystemPrompt,
    string Model,
    IReadOnlyList<AllowedCallback>? AllowedCallbacks = null,
    JsonNode? OutputSchema = null
);

/// <summary>
/// A single entry in the accumulated context passed to the LLM.
/// </summary>
public record LlmContextEntry(
    /// <summary>Human-readable description shown to the LLM.</summary>
    string Metadata,
    /// <summary>Arbitrary JSON payload.</summary>
    JsonNode? Payload
);

/// <summary>
/// Describes a callback the LLM is permitted to invoke during an agent step.
/// </summary>
public record AllowedCallback(
    string Name,
    /// <summary>Shown to the LLM as the tool description.</summary>
    string Description,
    /// <summary>Implementation invoked when the LLM calls this callback.</summary>
    Func<JsonNode, CancellationToken, Task<JsonNode?>> Handler,
    /// <summary>Informational mode label (e.g. "read", "write"). Appended to description if set.</summary>
    string? Mode = null,
    bool ApprovalRequired = false,
    /// <summary>JSON Schema for the tool's input. If null, the tool accepts an open-ended object.</summary>
    JsonNode? InputSchema = null
);

/// <summary>
/// Returned by RunAgentAsync when the agent step completes.
/// </summary>
public record StepResult(
    /// <summary>The output the LLM submitted via submit_result.</summary>
    JsonNode? Output,
    int LlmCallsUsed,
    decimal CostUsd,
    int TokensUsed,
    /// <summary>The model that was actually used for this step.</summary>
    string ModelUsed
);

/// <summary>
/// Internal config passed to the orchestrator, combining AgentDefinition + resolved policy.
/// </summary>
internal record AgentStepConfig(
    string Objective,
    string SystemPrompt,
    AgentRuntimePolicy Policy,
    string Model,
    IReadOnlyList<AllowedCallback>? AllowedCallbacks = null,
    JsonNode? OutputSchema = null,
    IReadOnlyList<LlmContextEntry>? LlmContext = null
);
