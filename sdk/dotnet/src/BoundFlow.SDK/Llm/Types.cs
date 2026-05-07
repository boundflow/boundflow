using System.Text.Json.Nodes;

namespace BoundFlow.SDK.Llm;

public record AgentPolicy(
    int MaxLlmCalls = 0,
    decimal MaxCostUsd = 0
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
/// Describes a callback the LLM is permitted to invoke during an agent step,
/// along with the implementation that runs when it is called.
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
    /// <summary>
    /// JSON Schema for the tool's input as a JsonNode. If null, the tool accepts an open-ended object.
    /// </summary>
    JsonNode? InputSchema = null
);

/// <summary>
/// Everything needed to run an agent step.
/// </summary>
public record AgentStepConfig(
    string Objective,
    /// <summary>System instruction given to the LLM. Required.</summary>
    string SystemPrompt,
    AgentPolicy Policy,
    IReadOnlyList<AllowedCallback>? AllowedCallbacks = null,
    /// <summary>
    /// JSON Schema for the submit_result tool output. If null, submit_result accepts an open-ended object.
    /// </summary>
    JsonNode? OutputSchema = null,
    IReadOnlyList<LlmContextEntry>? LlmContext = null
);

/// <summary>
/// Returned by Orchestrator.RunAsync when the agent step completes.
/// </summary>
public record StepResult(
    /// <summary>The output the LLM submitted via submit_result, conforming to OutputSchema.</summary>
    JsonNode? Output,
    int LlmCallsUsed,
    decimal CostUsd
);
