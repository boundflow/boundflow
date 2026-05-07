using System.Text.Json.Nodes;
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

    // ── Resources ─────────────────────────────────────────────────────────────

    public async Task<ResourceInstance> CreateResourceAsync(
        string resourceType,
        string tenantId,
        JsonNode initialState,
        int operationTimeoutSeconds,
        string correlationId = "",
        CancellationToken ct = default)
    {
        var resp = await _lifecycle.CreateResourceAsync(
            new CreateResourceRequest
            {
                ResourceType = resourceType,
                TenantId = tenantId,
                InitialState = ToStruct(initialState),
                OperationTimeoutSeconds = operationTimeoutSeconds,
                CorrelationId = correlationId,
            },
            cancellationToken: ct);
        return new ResourceInstance(
            resp.ResourceInstance.Id,
            resp.ResourceInstance.TenantId,
            FromStruct(resp.ResourceInstance.GoalState));
    }

    public async Task ReconcileResourceAsync(
        string resourceInstanceId,
        JsonNode goalState,
        int operationTimeoutSeconds,
        string correlationId = "",
        CancellationToken ct = default) =>
        await _lifecycle.ReconcileResourceAsync(
            new ReconcileResourceRequest
            {
                ResourceInstanceId = resourceInstanceId,
                GoalState = ToStruct(goalState),
                OperationTimeoutSeconds = operationTimeoutSeconds,
                CorrelationId = correlationId,
            },
            cancellationToken: ct);

    public async Task DeleteResourceAsync(
        string resourceInstanceId,
        int operationTimeoutSeconds,
        string correlationId = "",
        CancellationToken ct = default) =>
        await _lifecycle.DeleteResourceAsync(
            new DeleteResourceRequest
            {
                ResourceInstanceId = resourceInstanceId,
                OperationTimeoutSeconds = operationTimeoutSeconds,
                CorrelationId = correlationId,
            },
            cancellationToken: ct);

    public async Task<ResourceState> GetResourceStateAsync(string resourceInstanceId, CancellationToken ct = default)
    {
        var resp = await _lifecycle.GetResourceStateAsync(
            new GetResourceStateRequest { ResourceInstanceId = resourceInstanceId },
            cancellationToken: ct);
        return new ResourceState(
            FromStruct(resp.CurrentConfigState),
            FromStruct(resp.GoalConfigState),
            ParseLifecycleState(resp.LifecycleState));
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static Struct ToStruct(JsonNode node) =>
        JsonParser.Default.Parse<Struct>(node.ToJsonString());

    private static JsonNode? FromStruct(Struct? s) =>
        s is null ? null : JsonNode.Parse(JsonFormatter.Default.Format(s));

    private static LifecycleState ParseLifecycleState(string s) => s switch
    {
        "creating"    => LifecycleState.Creating,
        "active"      => LifecycleState.Active,
        "reconciling" => LifecycleState.Reconciling,
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
