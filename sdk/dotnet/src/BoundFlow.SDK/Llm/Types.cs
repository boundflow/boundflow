using System.Text.Json.Nodes;

namespace BoundFlow.SDK.Llm;

/// <summary>
/// Per-step runtime limits enforced by the orchestrator.
/// Server-stored values (loaded from agent_state context) take precedence over these.
/// </summary>
public record AgentRuntimePolicy(
    int MaxLlmCalls = 0,
    decimal MaxCostUsd = 0,
    /// <summary>Max tokens per individual LLM call. Defaults to 4096 if 0.</summary>
    int MaxTokensPerCall = 0,
    /// <summary>Max times any single tool may be called within the step. 0 = unlimited.</summary>
    int MaxCallsPerTool = 0,
    /// <summary>Restrict which models may be used. Null = use whatever model is configured.</summary>
    IReadOnlyList<string>? AllowedModels = null
);

/// <summary>
/// Describes a single adaptive rule. If the cumulative sum of <see cref="Metric"/> over
/// the last <see cref="Window"/> invocations satisfies the comparison, <see cref="Action"/>
/// is applied to the runtime policy before the next agent run.
/// </summary>
public record AgentLifecycleRule(
    AgentMetric Metric,
    PolicyOperator Operator,
    decimal Threshold,
    /// <summary>Rolling window in invocation count. 0 = evaluate against the current step only.</summary>
    int Window,
    PolicyMutation Action
);

public enum AgentMetric
{
    TokensUsed,
    CostUsd,
    LlmCalls,
    CallsPerTool,
}

public enum PolicyOperator
{
    LessThan,
    LessThanOrEqual,
    GreaterThan,
    GreaterThanOrEqual,
    Equal,
}

/// <summary>Mutates a single field of the runtime policy when a lifecycle rule fires.</summary>
public record PolicyMutation(PolicyField Field, object Value);

public enum PolicyField
{
    Model,
    MaxLlmCalls,
    MaxCostUsd,
    MaxTokensPerCall,
    MaxCallsPerTool,
}

/// <summary>
/// A set of adaptive rules evaluated before each agent run.
/// </summary>
public record AgentLifecyclePolicy(IReadOnlyList<AgentLifecycleRule> Rules);

/// <summary>
/// Inline definition of an agent step — system prompt, callbacks, and output schema.
/// Policies are loaded from agent_state in the operation context.
/// </summary>
public record AgentDefinition(
    string Name,
    string SystemPrompt,
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
    int TokensUsed
);

/// <summary>
/// Internal config passed to the orchestrator, combining AgentDefinition + resolved policy.
/// </summary>
internal record AgentStepConfig(
    string Objective,
    string SystemPrompt,
    AgentRuntimePolicy Policy,
    IReadOnlyList<AllowedCallback>? AllowedCallbacks = null,
    JsonNode? OutputSchema = null,
    IReadOnlyList<LlmContextEntry>? LlmContext = null
);
