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
    public static OperationResult Next(string operationName, JsonNode context, int timeoutSeconds)
        => new NextOperationResult(operationName, context, timeoutSeconds);
    public static OperationResult AwaitApproval(OperationResult onApprove, OperationResult onReject, int timeoutSeconds, string? justification = null)
        => new AwaitApprovalResult(onApprove, onReject, timeoutSeconds, justification);
}

public sealed record CompletedResult : OperationResult;
public sealed record NextOperationResult(string OperationName, JsonNode Context, int TimeoutSeconds) : OperationResult;
public sealed record AwaitApprovalResult(OperationResult OnApprove, OperationResult OnReject, int TimeoutSeconds, string? Justification = null) : OperationResult
{
    internal string ApprovalId { get; } = Guid.NewGuid().ToString();
}

public sealed record ApprovalRequest(
    string  WorkflowId,
    string  OperationName,
    int     TimeoutSeconds,
    string  ApprovalId,
    string? Justification = null
);

/// <summary>
/// Passed into every operation handler. Provides access to the operation data
/// and the ability to run an agent step.
/// </summary>
public sealed class OperationContext
{
    private readonly AtomicOperation _operation;
    private readonly Orchestrator _orchestrator;
    private readonly List<(string Key, LlmContextEntry Entry)> _llmContext = [];
    // Per-agent metrics from this operation, sent back to the server via AtomicOperationResult.
    internal readonly Dictionary<string, AgentInvocationMetrics> AgentStateUpdates = [];
    // Workflow-level failure signal for this operation. A failed run is still a completed
    // operation from BoundFlow's perspective; it just increments the num_failures metric.
    internal bool Failed { get; private set; }

    internal OperationContext(AtomicOperation operation, Orchestrator orchestrator)
    {
        _operation = operation ?? throw new ArgumentNullException(nameof(operation));
        _orchestrator = orchestrator;
        Context = JsonNode.Parse(JsonFormatter.Default.Format(_operation.Context))
            ?? throw new ArgumentException("Operation context JSON parsed to null.", nameof(operation));
    }

    /// <summary>The name of the current operation.</summary>
    public string Name => _operation.Name;

    /// <summary>The workflow version that triggered this invocation.</summary>
    public int WorkflowVersion => _operation.WorkflowVersion;

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
    /// Marks this run as a customer-side failure. The operation still completes normally
    /// from BoundFlow's perspective; this only increments the workflow's num_failures metric,
    /// which workflow lifecycle policies can act on.
    /// </summary>
    public void MarkFailed() => Failed = true;

    /// <summary>
    /// Runs an agent step inline. Policies are loaded from the server-stored agent_state
    /// in the operation context. Lifecycle rules are evaluated against invocation history
    /// before the run; metrics are accumulated and written back at operation completion.
    /// </summary>
    public async Task<StepResult> RunAgentAsync(AgentDefinition agent, CancellationToken ct = default)
    {
        // Runtime policy is snapshotted at request-creation time; lifecycle policy and
        // metrics are live values injected by the scheduler just before job dispatch.
        var runtimePolicyNode = Context["agentRuntimePolicies"]?[agent.Name];
        var agentStateNode    = Context["agentStates"]?[agent.Name];
        var runtimePolicy  = LifecyclePolicyEvaluator.LoadRuntimePolicy(runtimePolicyNode);
        var lifecyclePolicy = LifecyclePolicyEvaluator.LoadLifecyclePolicy(agentStateNode);
        var history         = LifecyclePolicyEvaluator.LoadMetricsHistory(agentStateNode);

        // Evaluate lifecycle rules and mutate policy accordingly.
        runtimePolicy = LifecyclePolicyEvaluator.ApplyLifecycleRules(lifecyclePolicy, history, runtimePolicy);

        var llmContext = _llmContext.Count > 0
            ? (IReadOnlyList<LlmContextEntry>)[.. _llmContext.Select(e => e.Entry)]
            : null;

        var effectiveModel = runtimePolicy.Model ?? agent.Model;

        var cfg = new AgentStepConfig(
            Objective: agent.Name,
            SystemPrompt: agent.SystemPrompt,
            Policy: runtimePolicy,
            Model: effectiveModel,
            AllowedCallbacks: agent.AllowedCallbacks,
            OutputSchema: agent.OutputSchema,
            LlmContext: llmContext
        );

        var result = await _orchestrator.RunAsync(cfg, ct);

        var newSnapshot = new InvocationSnapshot(
            TokensUsed:   result.TokensUsed,
            CostUsd:      (double)result.CostUsd,
            LlmCalls:     result.LlmCallsUsed,
            CallsPerTool: new Dictionary<string, int>(result.CallsPerTool),
            RanAt:        DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
        );

        var maxWindow = lifecyclePolicy?.Rules.Count > 0
            ? lifecyclePolicy.Rules.Max(r => r.Window)
            : 100;

        List<InvocationSnapshot> updatedHistory = [.. history, newSnapshot];
        while (updatedHistory.Count > Math.Max(maxWindow, 1))
            updatedHistory.RemoveAt(0);

        var agentMetrics = new AgentInvocationMetrics
        {
            CostUsd    = (double)result.CostUsd,
            LlmCalls   = result.LlmCallsUsed,
            TokensUsed = result.TokensUsed,
            RanAt      = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
        };
        foreach (var (tool, count) in result.CallsPerTool)
            agentMetrics.CallsPerTool[tool] = count;
        foreach (var (tool, count) in result.ToolFailureCounts)
            agentMetrics.ToolFailureCounts[tool] = count;
        AgentStateUpdates[agent.Name] = agentMetrics;

        return result;
    }

}

/// <summary>
/// Top-level customer-facing entry point. Register operation handlers by name,
/// then call RunAsync to start processing jobs.
/// </summary>
public sealed class BoundFlowWorker
{
    private readonly string _serverAddress;
    private readonly ILlmClient _llmClient;
    private readonly ILoggerFactory _loggerFactory;
    private readonly Dictionary<(string ResourceType, string OperationName), Func<OperationContext, CancellationToken, Task<OperationResult>>> _handlers = new();
    private readonly Dictionary<(string ResourceType, int Version), Func<OperationContext, CancellationToken, Task<OperationResult>>> _workflowHandlers = new();
    private Func<ApprovalRequest, CancellationToken, Task>? _approvalHandler;
    private const string EntryOperationName = "reconcile_entry";

    public BoundFlowWorker(
        string serverAddress,
        string llmApiKey,
        ILoggerFactory loggerFactory)
        : this(serverAddress, new AnthropicLlmClient(new AnthropicClient(new APIAuthentication(llmApiKey))), loggerFactory)
    {
    }

    /// <summary>
    /// Constructs a worker with a custom LLM client. Use this to inject a scripted mock
    /// for deterministic demos and tests instead of calling the real Anthropic API.
    /// </summary>
    public BoundFlowWorker(
        string serverAddress,
        ILlmClient llmClient,
        ILoggerFactory loggerFactory)
    {
        _serverAddress = serverAddress;
        _llmClient = llmClient;
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
    /// Registers the entry handler for a specific version of the given workflow type.
    /// Entry operations are dispatched by (resourceType, version); next operations by (resourceType, operationName).
    /// </summary>
    public BoundFlowWorker RegisterWorkflow(
        string resourceType,
        int version,
        Func<OperationContext, CancellationToken, Task<OperationResult>> handler)
    {
        _workflowHandlers[(resourceType, version)] = handler;
        return this;
    }

    /// <summary>
    /// Registers a callback invoked when any operation returns AwaitApproval.
    /// Use this to send notifications (Slack, email, etc.) to the approver.
    /// Exceptions in the callback are swallowed — a failed notification does not
    /// prevent the job from being parked awaiting approval.
    /// </summary>
    public BoundFlowWorker OnApprovalRequested(Func<ApprovalRequest, CancellationToken, Task> handler)
    {
        _approvalHandler = handler;
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
            Func<OperationContext, CancellationToken, Task<OperationResult>>? handler = null;
            if (op.Name == EntryOperationName)
                _workflowHandlers.TryGetValue((op.ResourceType, op.WorkflowVersion), out handler);
            else
                _handlers.TryGetValue((op.ResourceType, op.Name), out handler);

            if (handler is null)
                throw new InvalidOperationException($"No handler registered for resource type '{op.ResourceType}' operation '{op.Name}' version {op.WorkflowVersion}.");

            // New Orchestrator per call — stateless, cheap to create.
            var orchestrator = new Orchestrator(
                _llmClient,
                _loggerFactory.CreateLogger<Orchestrator>());

            var customerContext = new OperationContext(op, orchestrator);
            var result = await handler(customerContext, ct);

            if (result is AwaitApprovalResult approval && _approvalHandler is not null)
            {
                try
                {
                    await _approvalHandler(new ApprovalRequest(op.ResourceId, op.Name, approval.TimeoutSeconds, approval.ApprovalId, approval.Justification), ct);
                }
                catch (Exception ex)
                {
                    _loggerFactory.CreateLogger<BoundFlowWorker>()
                        .LogWarning(ex, "Approval notification callback threw. WorkflowId={WorkflowId}", op.ResourceId);
                }
            }

            var proto = MapToProto(result);
            foreach (var (name, metrics) in customerContext.AgentStateUpdates)
                proto.AgentStateUpdates.Add(name, metrics);
            if (customerContext.Failed)
                proto.WorkflowMetrics = new WorkflowInvocationMetrics { Failures = 1 };
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
        AwaitApprovalResult approval => new AtomicOperationResult
        {
            Status = OperationStatus.Completed,
            ApprovalGate = new ApprovalGate
            {
                OnApprove      = ToApprovalBranch(approval.OnApprove),
                OnReject       = ToApprovalBranch(approval.OnReject),
                TimeoutSeconds = approval.TimeoutSeconds,
                ApprovalId     = approval.ApprovalId ?? "",
            }
        },
        _ => throw new InvalidOperationException($"Unknown OperationResult type: {result.GetType().Name}")
    };

    private static AtomicOperation? ToApprovalBranch(OperationResult branch) => branch switch
    {
        CompletedResult       => null,
        NextOperationResult n => new AtomicOperation
        {
            Name           = n.OperationName,
            TimeoutSeconds = n.TimeoutSeconds,
            Context        = JsonParser.Default.Parse<Struct>(n.Context.ToJsonString())
        },
        _ => throw new InvalidOperationException($"Unsupported approval branch type: {branch.GetType().Name}")
    };
}
