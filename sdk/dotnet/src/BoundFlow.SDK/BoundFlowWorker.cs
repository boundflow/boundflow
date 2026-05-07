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
    /// Adds a context entry that will be included in the next RunAgentStepAsync call.
    /// Entries added here are merged with any LlmContext already in AgentStepConfig.
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
    /// Runs an LLM agent step inline within the current operation.
    /// Context entries added via AddLlmContext are merged into the config.
    /// Server-enforced policy from the operation context is merged over cfg.Policy.
    /// Audit metadata (LLM calls, cost) is recorded automatically.
    /// </summary>
    public Task<StepResult> RunAgentStepAsync(AgentStepConfig cfg, CancellationToken ct = default)
    {
        // TODO: read _boundflow_policy from Context and merge over cfg.Policy (server limits win)
        // TODO: store StepResult metadata (LlmCallsUsed, CostUsd) for audit bundling

        var mergedContext = _llmContext.Count > 0
            ? [.. (cfg.LlmContext ?? []), .. _llmContext.Select(e => e.Entry)]
            : cfg.LlmContext;

        var mergedCfg = cfg with { LlmContext = mergedContext };
        return _orchestrator.RunAsync(mergedCfg, ct);
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
    private readonly Dictionary<string, Func<OperationContext, CancellationToken, Task<OperationResult>>> _handlers = new();

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
    /// Registers a handler for the named operation.
    /// </summary>
    public BoundFlowWorker Register(
        string operationName,
        Func<OperationContext, CancellationToken, Task<OperationResult>> handler)
    {
        _handlers[operationName] = handler;
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
            if (!_handlers.TryGetValue(op.Name, out var handler))
                throw new InvalidOperationException($"No handler registered for operation '{op.Name}'.");

            // New Orchestrator per call — stateless, cheap to create.
            var orchestrator = new Orchestrator(
                _anthropicClient,
                _llmModel,
                _loggerFactory.CreateLogger<Orchestrator>());

            var customerContext = new OperationContext(op, orchestrator);
            var result = await handler(customerContext, ct);
            return MapToProto(result);
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
