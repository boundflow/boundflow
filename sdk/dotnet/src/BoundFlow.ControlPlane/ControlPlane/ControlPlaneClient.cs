using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.Json.Serialization;
using Convergeplane.V1;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using Grpc.Net.Client;

namespace BoundFlow.ControlPlane;

/// <summary>
/// Client for the BoundFlow control plane — registration and resource lifecycle APIs.
/// </summary>
public sealed class ControlPlaneClient : IDisposable
{
    private readonly GrpcChannel _channel;
    private readonly RegistrationService.RegistrationServiceClient _registration;
    private readonly ResourceLifecycleService.ResourceLifecycleServiceClient _lifecycle;

    public ControlPlaneClient(string serverAddress)
    {
        _channel = GrpcChannel.ForAddress(serverAddress);
        _registration = new RegistrationService.RegistrationServiceClient(_channel);
        _lifecycle = new ResourceLifecycleService.ResourceLifecycleServiceClient(_channel);
    }

    // ── Tenant Groups ─────────────────────────────────────────────────────────

    public async Task<TenantGroup> CreateTenantGroupAsync(string name, CancellationToken ct = default)
    {
        var resp = await _registration.CreateTenantGroupAsync(
            new CreateTenantGroupRequest { TenantGroup = new Convergeplane.V1.TenantGroup { Name = name } },
            cancellationToken: ct);
        return new TenantGroup(resp.TenantGroup.Id, resp.TenantGroup.Name);
    }

    public async Task<TenantGroup> GetTenantGroupAsync(string id, CancellationToken ct = default)
    {
        var resp = await _registration.GetTenantGroupAsync(
            new GetTenantGroupRequest { Id = id },
            cancellationToken: ct);
        return new TenantGroup(resp.TenantGroup.Id, resp.TenantGroup.Name);
    }

    public async Task DeleteTenantGroupAsync(string id, CancellationToken ct = default) =>
        await _registration.DeleteTenantGroupAsync(
            new DeleteTenantGroupRequest { Id = id },
            cancellationToken: ct);

    // ── Tenants ───────────────────────────────────────────────────────────────

    public async Task<Tenant> CreateTenantAsync(string name, CancellationToken ct = default)
    {
        var resp = await _registration.CreateTenantAsync(
            new CreateTenantRequest { Tenant = new Convergeplane.V1.Tenant { Name = name } },
            cancellationToken: ct);
        return new Tenant(resp.Tenant.Id, resp.Tenant.Name, resp.Tenant.TenantGroupId);
    }

    public async Task<Tenant> GetTenantAsync(string id, CancellationToken ct = default)
    {
        var resp = await _registration.GetTenantAsync(
            new GetTenantRequest { Id = id },
            cancellationToken: ct);
        return new Tenant(resp.Tenant.Id, resp.Tenant.Name, resp.Tenant.TenantGroupId);
    }

    public async Task DeleteTenantAsync(string id, CancellationToken ct = default) =>
        await _registration.DeleteTenantAsync(
            new DeleteTenantRequest { Id = id },
            cancellationToken: ct);

    // ── Workflows ─────────────────────────────────────────────────────────────

    public async Task<Workflow> CreateWorkflowAsync(
        string workflowType,
        string tenantId,
        WorkflowConfig? workflowConfig = null,
        string correlationId = "",
        CancellationToken ct = default)
    {
        var cfg = workflowConfig ?? new WorkflowConfig();
        var resp = await _lifecycle.CreateResourceAsync(
            new CreateResourceRequest
            {
                ResourceType = workflowType,
                TenantId = tenantId,
                CorrelationId = correlationId,
                WorkflowConfig = new Convergeplane.V1.WorkflowConfig
                {
                    Version              = cfg.Version,
                    InvokeTimeoutSeconds = cfg.InvokeTimeoutSeconds,
                    RepeatEverySeconds   = cfg.RepeatEverySeconds,
                    Triggerable          = cfg.Triggerable,
                },
            },
            cancellationToken: ct);
        var ri = resp.ResourceInstance;
        return new Workflow(
            ri.Id,
            ri.TenantId,
            new WorkflowConfig(
                ri.WorkflowConfig?.Version ?? 0,
                ri.WorkflowConfig?.InvokeTimeoutSeconds ?? 60,
                ri.WorkflowConfig?.RepeatEverySeconds ?? 0,
                ri.WorkflowConfig?.Triggerable ?? true));
    }

    public async Task InvokeWorkflowAsync(
        string workflowId,
        RuntimeOverrides? overrides = null,
        string correlationId = "",
        CancellationToken ct = default) =>
        await _lifecycle.ReconcileResourceAsync(
            new ReconcileResourceRequest
            {
                ResourceInstanceId = workflowId,
                CorrelationId = correlationId,
                RuntimeOverrides = overrides == null ? null : new Convergeplane.V1.RuntimeOverrides
                {
                    OperationTimeoutSeconds = overrides.OperationTimeoutSeconds,
                },
            },
            cancellationToken: ct);

    public async Task DeleteWorkflowAsync(
        string workflowId,
        string correlationId = "",
        CancellationToken ct = default) =>
        await _lifecycle.DeleteResourceAsync(
            new DeleteResourceRequest
            {
                ResourceInstanceId = workflowId,
                CorrelationId = correlationId,
            },
            cancellationToken: ct);

    public async Task<WorkflowState?> GetWorkflowStateAsync(string workflowId, CancellationToken ct = default)
    {
        var resp = await _lifecycle.GetResourceStateAsync(
            new GetResourceStateRequest { ResourceInstanceId = workflowId },
            cancellationToken: ct);
        if (resp.ResourceInstance?.LifecycleState == "deleted")
            return null;
        return ParseWorkflowState(resp.ResourceInstance?.WorkflowState ?? Convergeplane.V1.WorkflowState.Unspecified);
    }

    public async Task ActivateWorkflowAsync(string workflowId, CancellationToken ct = default) =>
        await _lifecycle.ActivateWorkflowAsync(
            new ActivateWorkflowRequest { ResourceInstanceId = workflowId },
            cancellationToken: ct);

    public async Task<LifecycleState> GetWorkflowLifecycleStateAsync(string workflowId, CancellationToken ct = default)
    {
        var resp = await _lifecycle.GetResourceStateAsync(
            new GetResourceStateRequest { ResourceInstanceId = workflowId },
            cancellationToken: ct);
        return resp.ResourceInstance?.LifecycleState switch
        {
            "creating"    => LifecycleState.Creating,
            "active"      => LifecycleState.Active,
            "reconciling"        => LifecycleState.Invoking,
            "awaiting_approval"  => LifecycleState.AwaitingApproval,
            "deleting"           => LifecycleState.Deleting,
            "deleted"     => LifecycleState.Deleted,
            "failed"      => LifecycleState.Failed,
            _             => LifecycleState.Unknown,
        };
    }

    public async Task SetWorkflowLifecyclePolicyAsync(
        string workflowId,
        WorkflowLifecyclePolicy policy,
        CancellationToken ct = default)
    {
        await _lifecycle.SetWorkflowLifecyclePolicyAsync(
            new SetWorkflowLifecyclePolicyRequest
            {
                ResourceInstanceId = workflowId,
                LifecyclePolicy = ToWorkflowLifecyclePolicyProto(policy),
            },
            cancellationToken: ct);
    }

    public async Task ApproveWorkflowAsync(string workflowId, string approvalId, CancellationToken ct = default) =>
        await _lifecycle.ApproveWorkflowAsync(
            new ApproveWorkflowRequest { ResourceInstanceId = workflowId, ApprovalId = approvalId },
            cancellationToken: ct);

    public async Task RejectWorkflowAsync(string workflowId, string approvalId, CancellationToken ct = default) =>
        await _lifecycle.RejectWorkflowAsync(
            new RejectWorkflowRequest { ResourceInstanceId = workflowId, ApprovalId = approvalId },
            cancellationToken: ct);

    // ── Agent state ──────────────────────────────────────────────────────────

    public async Task SetAgentRuntimePolicyAsync(
        string workflowId,
        string agentName,
        AgentRuntimePolicy runtimePolicy,
        CancellationToken ct = default) =>
        await _lifecycle.SetAgentRuntimePolicyAsync(
            new SetAgentRuntimePolicyRequest
            {
                ResourceInstanceId = workflowId,
                AgentName = agentName,
                RuntimePolicy = ToStruct(SerializePolicy(runtimePolicy)),
            },
            cancellationToken: ct);

    public async Task SetAgentLifecyclePolicyAsync(
        string workflowId,
        string agentName,
        AgentLifecyclePolicy lifecyclePolicy,
        CancellationToken ct = default) =>
        await _lifecycle.SetAgentLifecyclePolicyAsync(
            new SetAgentLifecyclePolicyRequest
            {
                ResourceInstanceId = workflowId,
                AgentName = agentName,
                LifecyclePolicy = ToStruct(SerializePolicy(lifecyclePolicy)),
            },
            cancellationToken: ct);

    public async Task DeleteAgentAsync(
        string workflowId,
        string agentName,
        CancellationToken ct = default) =>
        await _lifecycle.DeleteAgentAsync(
            new DeleteAgentRequest
            {
                ResourceInstanceId = workflowId,
                AgentName = agentName,
            },
            cancellationToken: ct);

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static readonly JsonSerializerOptions PolicySerializerOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower,
        Converters = { new JsonStringEnumConverter(JsonNamingPolicy.CamelCase) },
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
    };

    private static JsonNode SerializePolicy<T>(T policy) =>
        JsonSerializer.SerializeToNode(policy, PolicySerializerOptions)!;

    private static Struct ToStruct(JsonNode node) =>
        JsonParser.Default.Parse<Struct>(node.ToJsonString());

    private static JsonNode? FromStruct(Struct? s) =>
        s is null ? null : JsonNode.Parse(JsonFormatter.Default.Format(s));

    private static WorkflowState ParseWorkflowState(Convergeplane.V1.WorkflowState s) => s switch
    {
        Convergeplane.V1.WorkflowState.Active   => WorkflowState.Active,
        Convergeplane.V1.WorkflowState.Paused   => WorkflowState.Paused,
        Convergeplane.V1.WorkflowState.Cooldown => WorkflowState.Cooldown,
        Convergeplane.V1.WorkflowState.Disabled => WorkflowState.Disabled,
        _                                        => WorkflowState.Unspecified,
    };

    private static Convergeplane.V1.WorkflowLifecyclePolicy ToWorkflowLifecyclePolicyProto(WorkflowLifecyclePolicy policy)
    {
        var proto = new Convergeplane.V1.WorkflowLifecyclePolicy();
        foreach (var rule in policy.Rules)
        {
            var protoRule = new Convergeplane.V1.WorkflowLifecyclePolicyRule
            {
                Metric   = (Convergeplane.V1.WorkflowMetric)rule.Metric,
                Threshold = rule.Threshold,
                ToolName  = rule.ToolName ?? "",
            };
            protoRule.Action = rule.Action switch
            {
                PauseAction p => new Convergeplane.V1.WorkflowLifecyclePolicyAction
                {
                    Type = Convergeplane.V1.WorkflowPolicyActionType.WorkflowPolicyActionPause,
                },
                CooldownAction c => new Convergeplane.V1.WorkflowLifecyclePolicyAction
                {
                    Type            = Convergeplane.V1.WorkflowPolicyActionType.WorkflowPolicyActionCooldown,
                    CooldownSeconds = c.CooldownSeconds,
                },
                SetVersionAction s => new Convergeplane.V1.WorkflowLifecyclePolicyAction
                {
                    Type          = Convergeplane.V1.WorkflowPolicyActionType.WorkflowPolicyActionSetVersion,
                    TargetVersion = s.TargetVersion,
                },
                _ => throw new ArgumentException($"Unknown action type: {rule.Action.GetType().Name}"),
            };
            protoRule.Window = rule.Action switch
            {
                PauseAction p    => p.Window,
                CooldownAction c => c.Window,
                _                => 0,
            };
            proto.Rules.Add(protoRule);
        }
        return proto;
    }

    public void Dispose()
    {
        _channel.Dispose();
    }
}
