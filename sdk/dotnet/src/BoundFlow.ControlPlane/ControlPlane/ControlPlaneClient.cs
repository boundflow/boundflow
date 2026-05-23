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

    public async Task<Tenant> CreateTenantAsync(string name, string tenantGroupId, CancellationToken ct = default)
    {
        var resp = await _registration.CreateTenantAsync(
            new CreateTenantRequest { Tenant = new Convergeplane.V1.Tenant { Name = name, TenantGroupId = tenantGroupId } },
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
                    InitialVersion       = cfg.InitialVersion,
                    InvokeTimeoutSeconds = cfg.InvokeTimeoutSeconds,
                    RepeatEverySeconds   = cfg.RepeatEverySeconds,
                    Triggerable          = cfg.Triggerable,
                },
            },
            cancellationToken: ct);
        var retCfg = resp.WorkflowConfig;
        return new Workflow(
            resp.ResourceInstance.Id,
            resp.ResourceInstance.TenantId,
            new WorkflowConfig(
                retCfg.InitialVersion,
                retCfg.InvokeTimeoutSeconds,
                retCfg.RepeatEverySeconds,
                retCfg.Triggerable));
    }

    public async Task InvokeWorkflowAsync(
        string workflowId,
        JsonNode goalState,
        int operationTimeoutSeconds,
        string correlationId = "",
        CancellationToken ct = default) =>
        await _lifecycle.ReconcileResourceAsync(
            new ReconcileResourceRequest
            {
                ResourceInstanceId = workflowId,
                GoalState = ToStruct(goalState),
                OperationTimeoutSeconds = operationTimeoutSeconds,
                CorrelationId = correlationId,
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

    public async Task<WorkflowState> GetWorkflowStateAsync(string workflowId, CancellationToken ct = default)
    {
        var resp = await _lifecycle.GetResourceStateAsync(
            new GetResourceStateRequest { ResourceInstanceId = workflowId },
            cancellationToken: ct);
        return new WorkflowState(
            FromStruct(resp.CurrentConfigState),
            FromStruct(resp.GoalConfigState),
            ParseLifecycleState(resp.LifecycleState));
    }

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

    private static LifecycleState ParseLifecycleState(string s) => s switch
    {
        "creating"    => LifecycleState.Creating,
        "active"      => LifecycleState.Active,
        "reconciling" => LifecycleState.Invoking,
        "deleting"    => LifecycleState.Deleting,
        "deleted"     => LifecycleState.Deleted,
        "failed"      => LifecycleState.Failed,
        _             => LifecycleState.Unknown,
    };

    public void Dispose()
    {
        _channel.Dispose();
    }
}
