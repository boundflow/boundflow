using System.Text.Json.Nodes;
using Anthropic.SDK;
using BoundFlow.SDK.Llm;
using BoundFlow.SDK.Worker;
using Convergeplane.V1;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using Microsoft.Extensions.Logging;

namespace BoundFlow.SDK;

/// <summary>
/// The result an operation handler returns to signal what happens next.
/// </summary>
public abstract record OperationResult
{
    public static OperationResult Complete() => new CompletedResult();
    public static OperationResult Next(string operationName, JsonNode context, int timeoutSeconds = 0)
        => new NextOperationResult(operationName, context, timeoutSeconds);
}

public sealed record CompletedResult : OperationResult;
public sealed record NextOperationResult(string OperationName, JsonNode Context, int TimeoutSeconds) : OperationResult;

/// <summary>
/// Passed into every operation handler. Provides access to the operation data
/// and the ability to run an agent step.
/// </summary>
public sealed class OperationContext
{
    private readonly AtomicOperation _operation;
    private readonly Orchestrator _orchestrator;
    private readonly List<(string Key, LlmContextEntry Entry)> _llmContext = [];
    // Pending agent state updates to be written back via AtomicOperationResult.
    internal readonly Dictionary<string, JsonNode> AgentStateUpdates = [];

    internal OperationContext(AtomicOperation operation, Orchestrator orchestrator)
    {
        _operation = operation ?? throw new ArgumentNullException(nameof(operation));
        _orchestrator = orchestrator;
        Context = JsonNode.Parse(JsonFormatter.Default.Format(_operation.Context))
            ?? throw new ArgumentException("Operation context JSON parsed to null.", nameof(operation));
    }

    /// <summary>The name of the current operation.</summary>
    public string Name => _operation.Name;

    /// <summary>The context data attached to this operation by the server or the previous step.</summary>
    public readonly JsonNode Context;

    /// <summary>
    /// Adds a context entry that will be included in the next RunAgentAsync call.
    /// key defaults to metadata if not provided and is used to remove the entry later.
    /// </summary>
    public OperationContext AddLlmContext(string metadata, JsonNode? payload, string? key = null)
    {
        _llmContext.Add((key ?? metadata, new LlmContextEntry(metadata, payload)));
        return this;
    }

    /// <summary>Removes a previously added context entry by key.</summary>
    public OperationContext RemoveLlmContext(string key)
    {
        _llmContext.RemoveAll(e => e.Key == key);
        return this;
    }

    /// <summary>
    /// Runs an agent step inline. Policies are loaded from the server-stored agent_state
    /// in the operation context. Lifecycle rules are evaluated against invocation history
    /// before the run; metrics are accumulated and written back at operation completion.
    /// </summary>
    public async Task<StepResult> RunAgentAsync(AgentDefinition agent, CancellationToken ct = default)
    {
        // Load server-stored agent state from context.
        var agentStateNode = Context["_bf_agent_state"]?[agent.Name];
        var runtimePolicy = LoadRuntimePolicy(agentStateNode);
        var lifecyclePolicy = LoadLifecyclePolicy(agentStateNode);
        var history = LoadMetricsHistory(agentStateNode);

        // Evaluate lifecycle rules and mutate policy accordingly.
        runtimePolicy = ApplyLifecycleRules(lifecyclePolicy, history, runtimePolicy);

        var llmContext = _llmContext.Count > 0
            ? (IReadOnlyList<LlmContextEntry>)[.. _llmContext.Select(e => e.Entry)]
            : null;

        var cfg = new AgentStepConfig(
            Objective: agent.Name,
            SystemPrompt: agent.SystemPrompt,
            Policy: runtimePolicy,
            AllowedCallbacks: agent.AllowedCallbacks,
            OutputSchema: agent.OutputSchema,
            LlmContext: llmContext
        );

        var result = await _orchestrator.RunAsync(cfg, ct);

        // Append new metric snapshot and update the in-memory state for return.
        var newSnapshot = new JsonObject
        {
            ["tokens_used"]    = result.TokensUsed,
            ["cost_usd"]       = (double)result.CostUsd,
            ["llm_calls"]      = result.LlmCallsUsed,
            ["calls_per_tool"] = 0,
            ["ran_at"]         = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
        };

        var maxWindow = lifecyclePolicy?.Rules.Count > 0
            ? lifecyclePolicy.Rules.Max(r => r.Window)
            : 100;

        var updatedHistory = new JsonArray([.. history, newSnapshot]);
        while (updatedHistory.Count > Math.Max(maxWindow, 1))
            updatedHistory.RemoveAt(0);

        AgentStateUpdates[agent.Name] = new JsonObject
        {
            ["invocation_metrics"] = updatedHistory,
        };

        return result;
    }

    // ── Private helpers ───────────────────────────────────────────────────────

    private static AgentRuntimePolicy LoadRuntimePolicy(JsonNode? stateNode)
    {
        if (stateNode?["runtime_policy"] is not JsonObject rp) return new AgentRuntimePolicy();
        return new AgentRuntimePolicy(
            MaxLlmCalls:      rp["max_llm_calls"]?.GetValue<int>()           ?? 0,
            MaxCostUsd:       (decimal)(rp["max_cost_usd"]?.GetValue<double>() ?? 0),
            MaxTokensPerCall: rp["max_tokens_per_call"]?.GetValue<int>()     ?? 0,
            MaxCallsPerTool:  rp["max_calls_per_tool"]?.GetValue<int>()      ?? 0
        );
    }

    private static AgentLifecyclePolicy? LoadLifecyclePolicy(JsonNode? stateNode)
    {
        if (stateNode?["lifecycle_policy"]?["rules"] is not JsonArray rulesNode) return null;
        var rules = rulesNode
            .OfType<JsonObject>()
            .Select(r => new AgentLifecycleRule(
                Metric:    System.Enum.Parse<AgentMetric>(r["metric"]?.GetValue<string>() ?? "TokensUsed", ignoreCase: true),
                Operator:  System.Enum.Parse<PolicyOperator>(r["operator"]?.GetValue<string>() ?? "GreaterThan", ignoreCase: true),
                Threshold: (decimal)(r["threshold"]?.GetValue<double>() ?? 0),
                Window:    r["window"]?.GetValue<int>() ?? 0,
                Action: new PolicyMutation(
                    System.Enum.Parse<PolicyField>(r["action"]?["field"]?.GetValue<string>() ?? "Model", ignoreCase: true),
                    r["action"]?["value"]?.ToString() ?? ""
                )
            ))
            .ToList();
        return new AgentLifecyclePolicy(rules);
    }

    private static List<JsonNode> LoadMetricsHistory(JsonNode? stateNode)
    {
        if (stateNode?["invocation_metrics"] is not JsonArray arr) return [];
        return [.. arr.OfType<JsonNode>()];
    }

    private static AgentRuntimePolicy ApplyLifecycleRules(
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

    private static decimal GetMetricValue(JsonNode entry, AgentMetric metric) => metric switch
    {
        AgentMetric.TokensUsed    => (decimal)(entry["tokens_used"]?.GetValue<double>()    ?? 0),
        AgentMetric.CostUsd       => (decimal)(entry["cost_usd"]?.GetValue<double>()       ?? 0),
        AgentMetric.LlmCalls      => (decimal)(entry["llm_calls"]?.GetValue<double>()      ?? 0),
        AgentMetric.CallsPerTool  => (decimal)(entry["calls_per_tool"]?.GetValue<double>() ?? 0),
        _                         => 0,
    };

    private static bool Evaluate(decimal sum, PolicyOperator op, decimal threshold) => op switch
    {
        PolicyOperator.LessThan           => sum < threshold,
        PolicyOperator.LessThanOrEqual    => sum <= threshold,
        PolicyOperator.GreaterThan        => sum > threshold,
        PolicyOperator.GreaterThanOrEqual => sum >= threshold,
        PolicyOperator.Equal              => sum == threshold,
        _                                 => false,
    };

    private static AgentRuntimePolicy ApplyMutation(AgentRuntimePolicy policy, PolicyMutation mutation)
    {
        var val = mutation.Value?.ToString() ?? "";
        return mutation.Field switch
        {
            PolicyField.MaxLlmCalls      => policy with { MaxLlmCalls      = int.TryParse(val, out var i)     ? i : policy.MaxLlmCalls },
            PolicyField.MaxCostUsd       => policy with { MaxCostUsd       = decimal.TryParse(val, out var d) ? d : policy.MaxCostUsd },
            PolicyField.MaxTokensPerCall => policy with { MaxTokensPerCall = int.TryParse(val, out var t)     ? t : policy.MaxTokensPerCall },
            PolicyField.MaxCallsPerTool  => policy with { MaxCallsPerTool  = int.TryParse(val, out var c)     ? c : policy.MaxCallsPerTool },
            // Model mutations are passed through to the orchestrator via AllowedModels check.
            _                            => policy,
        };
    }
}

/// <summary>
/// Top-level customer-facing entry point. Register operation handlers by name,
/// then call RunAsync to start processing jobs.
/// </summary>
public sealed class BoundFlowWorker
{
    private readonly string _serverAddress;
    private readonly AnthropicClient _anthropicClient;
    private readonly string _llmModel;
    private readonly ILoggerFactory _loggerFactory;
    private readonly Dictionary<(string ResourceType, string OperationName), Func<OperationContext, CancellationToken, Task<OperationResult>>> _handlers = new();

    public BoundFlowWorker(
        string serverAddress,
        string llmApiKey,
        ILoggerFactory loggerFactory,
        string llmModel = "claude-sonnet-4-6")
    {
        _serverAddress = serverAddress;
        _anthropicClient = new AnthropicClient(new APIAuthentication(llmApiKey));
        _llmModel = llmModel;
        _loggerFactory = loggerFactory;
    }

    /// <summary>
    /// Registers a handler for a specific resource type and operation name.
    /// </summary>
    public BoundFlowWorker Register(
        string resourceType,
        string operationName,
        Func<OperationContext, CancellationToken, Task<OperationResult>> handler)
    {
        _handlers[(resourceType, operationName)] = handler;
        return this;
    }

    /// <summary>
    /// Connects to the server and processes jobs until cancellation.
    /// </summary>
    public Task RunAsync(CancellationToken ct = default)
    {
        var workerClient = new WorkerClient(_serverAddress, _loggerFactory.CreateLogger<WorkerClient>());

        OperationHandler operationHandler = async (op, ct) =>
        {
            if (!_handlers.TryGetValue((op.ResourceType, op.Name), out var handler))
                throw new InvalidOperationException($"No handler registered for resource type '{op.ResourceType}' operation '{op.Name}'.");

            // New Orchestrator per call — stateless, cheap to create.
            var orchestrator = new Orchestrator(
                _anthropicClient,
                _llmModel,
                _loggerFactory.CreateLogger<Orchestrator>());

            var customerContext = new OperationContext(op, orchestrator);
            var result = await handler(customerContext, ct);
            var proto = MapToProto(result);
            if (customerContext.AgentStateUpdates.Count > 0)
            {
                var updatesObj = new JsonObject();
                foreach (var (name, state) in customerContext.AgentStateUpdates)
                    updatesObj[name] = state.DeepClone();
                proto.AgentStateUpdates = JsonParser.Default.Parse<Struct>(updatesObj.ToJsonString());
            }
            return proto;
        };

        return workerClient.RunAsync(operationHandler, ct);
    }

    private static AtomicOperationResult MapToProto(OperationResult result) => result switch
    {
        CompletedResult => new AtomicOperationResult { Status = OperationStatus.Completed },
        NextOperationResult next => new AtomicOperationResult
        {
            Status = OperationStatus.Completed,
            NextOperation = new AtomicOperation
            {
                Name = next.OperationName,
                TimeoutSeconds = next.TimeoutSeconds,
                Context = JsonParser.Default.Parse<Struct>(next.Context.ToJsonString())
            }
        },
        _ => throw new InvalidOperationException($"Unknown OperationResult type: {result.GetType().Name}")
    };
}
