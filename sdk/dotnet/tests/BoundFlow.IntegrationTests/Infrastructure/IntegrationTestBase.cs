using BoundFlow.ControlPlane;
using Microsoft.Extensions.Logging;
using Xunit;

namespace BoundFlow.IntegrationTests.Infrastructure;

public abstract class IntegrationTestBase : IAsyncLifetime
{
    protected const string WorkerAddress = "http://localhost:50052";
    protected const string ServerAddress = "http://localhost:50051";

    protected static string LlmApiKey =>
        Environment.GetEnvironmentVariable("ANTHROPIC_API_KEY")
        ?? throw new InvalidOperationException("ANTHROPIC_API_KEY is required.");

    protected readonly ILoggerFactory LoggerFactory;
    protected readonly ControlPlaneClient ControlPlane;

    protected IntegrationTestBase()
    {
        LoggerFactory = Microsoft.Extensions.Logging.LoggerFactory.Create(b =>
            b.AddConsole().SetMinimumLevel(LogLevel.Warning));
        ControlPlane = new ControlPlaneClient(ServerAddress);
    }

    protected async Task<(TenantGroup Group, Tenant Tenant)> CreateIsolatedTenantAsync(string prefix = "test")
    {
        var id = Guid.NewGuid().ToString("N")[..8];
        var group = await ControlPlane.CreateTenantGroupAsync($"{prefix}-group-{id}");
        var tenant = await ControlPlane.CreateTenantAsync($"{prefix}-tenant-{id}", group.Id);
        return (group, tenant);
    }

    protected async Task<LifecycleState> WaitForCompletionAsync(string workflowId, CancellationToken ct = default)
    {
        LifecycleState state;
        do
        {
            await Task.Delay(500, ct);
            state = await ControlPlane.GetWorkflowLifecycleStateAsync(workflowId, ct);
        }
        while (state == LifecycleState.Invoking);
        return state;
    }

    protected async Task<WorkflowState> WaitForWorkflowStateAsync(string workflowId, WorkflowState expected, CancellationToken ct = default)
    {
        WorkflowState? state;
        do
        {
            await Task.Delay(500, ct);
            state = await ControlPlane.GetWorkflowStateAsync(workflowId, ct);
        }
        while (state != expected);
        return state!.Value;
    }

    public virtual Task InitializeAsync() => Task.CompletedTask;

    public virtual async Task DisposeAsync()
    {
        ControlPlane.Dispose();
        LoggerFactory.Dispose();
        await Task.CompletedTask;
    }
}
