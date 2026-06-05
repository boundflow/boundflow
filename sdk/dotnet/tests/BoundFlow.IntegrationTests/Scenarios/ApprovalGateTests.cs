using System.Text.Json.Nodes;
using BoundFlow.ControlPlane;
using BoundFlow.IntegrationTests.Infrastructure;
using BoundFlow.SDK;
using Xunit;

namespace BoundFlow.IntegrationTests.Scenarios;

public class ApprovalGateTests : IntegrationTestBase
{
    /// <summary>
    /// Verifies the happy path: a workflow parks for approval, the notification
    /// callback fires with the approval ID, and approving resumes the on_approve operation.
    /// </summary>
    [Fact]
    public async Task ApproveGate_RunsApproveOperation()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        ApprovalRequest? capturedRequest = null;
        var approvedStepRan = false;

        var worker = new BoundFlowWorker(WorkerAddress, "dummy-key-not-used", LoggerFactory)
            .RegisterWorkflow("approval_approve", 1, (ctx, ct) =>
                Task.FromResult(OperationResult.AwaitApproval(
                    onApprove:      OperationResult.Next("approved_step", ctx.Context, 30),
                    onReject:       OperationResult.Complete(),
                    timeoutSeconds: 60,
                    justification:  "needs human sign-off")))
            .Register("approval_approve", "approved_step", (ctx, ct) =>
            {
                approvedStepRan = true;
                return Task.FromResult(OperationResult.Complete());
            })
            .OnApprovalRequested((request, ct) =>
            {
                capturedRequest = request;
                return Task.CompletedTask;
            });

        var workerTask = worker.RunAsync(cts.Token);

        var (_, tenant) = await CreateIsolatedTenantAsync("approval-approve");
        var workflow = await ControlPlane.CreateWorkflowAsync("approval_approve", tenant.Id,
            workflowConfig: new WorkflowConfig(Version: 1));

        await ControlPlane.ActivateWorkflowAsync(workflow.Id);
        await ControlPlane.InvokeWorkflowAsync(workflow.Id, new RuntimeOverrides(OperationTimeoutSeconds: 30));

        // Wait for the workflow to park awaiting approval.
        await WaitForLifecycleStateAsync(workflow.Id, LifecycleState.AwaitingApproval, cts.Token);

        Assert.NotNull(capturedRequest);
        Assert.Equal(workflow.Id, capturedRequest.WorkflowId);
        Assert.Equal("needs human sign-off", capturedRequest.Justification);
        Assert.NotEmpty(capturedRequest.ApprovalId);

        // Approve — should resume the approved_step operation.
        await ControlPlane.ApproveWorkflowAsync(workflow.Id, capturedRequest.ApprovalId, cts.Token);
        await WaitForCompletionAsync(workflow.Id, cts.Token);

        Assert.True(approvedStepRan, "approved_step should have run after approval.");

        cts.Cancel();
        try { await workerTask; } catch { }
    }

    /// <summary>
    /// Verifies that an approval gate auto-rejects and runs the on_reject branch
    /// once the approval timeout expires, without any explicit reject call.
    /// </summary>
    [Fact]
    public async Task ApprovalTimeout_RunsRejectOperation()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        var timedOutStepRan = false;
        var approvedStepRan = false;

        var worker = new BoundFlowWorker(WorkerAddress, "dummy-key-not-used", LoggerFactory)
            .RegisterWorkflow("approval_timeout", 1, (ctx, ct) =>
                Task.FromResult(OperationResult.AwaitApproval(
                    onApprove:      OperationResult.Next("approved_step", ctx.Context, 30),
                    onReject:       OperationResult.Next("timed_out_step", ctx.Context, 30),
                    timeoutSeconds: 8)))
            .Register("approval_timeout", "approved_step", (ctx, ct) =>
            {
                approvedStepRan = true;
                return Task.FromResult(OperationResult.Complete());
            })
            .Register("approval_timeout", "timed_out_step", (ctx, ct) =>
            {
                timedOutStepRan = true;
                return Task.FromResult(OperationResult.Complete());
            });

        var workerTask = worker.RunAsync(cts.Token);

        var (_, tenant) = await CreateIsolatedTenantAsync("approval-timeout");
        var workflow = await ControlPlane.CreateWorkflowAsync("approval_timeout", tenant.Id,
            workflowConfig: new WorkflowConfig(Version: 1));

        await ControlPlane.ActivateWorkflowAsync(workflow.Id);
        await ControlPlane.InvokeWorkflowAsync(workflow.Id, new RuntimeOverrides(OperationTimeoutSeconds: 30));

        // Wait for it to park, then let the timeout expire — don't call approve or reject.
        await WaitForLifecycleStateAsync(workflow.Id, LifecycleState.AwaitingApproval, cts.Token);
        await WaitForLifecycleStateAsync(workflow.Id, LifecycleState.Active, cts.Token);

        Assert.True(timedOutStepRan, "timed_out_step should have run after timeout.");
        Assert.False(approvedStepRan, "approved_step should NOT have run.");

        cts.Cancel();
        try { await workerTask; } catch { }
    }

    /// <summary>
    /// Verifies the reject path: rejecting the approval runs the on_reject branch
    /// (Complete in this case) and the on_approve operation does not run.
    /// </summary>
    [Fact]
    public async Task RejectGate_SkipsApproveOperation()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        ApprovalRequest? capturedRequest = null;
        var approvedStepRan = false;

        var worker = new BoundFlowWorker(WorkerAddress, "dummy-key-not-used", LoggerFactory)
            .RegisterWorkflow("approval_reject", 1, (ctx, ct) =>
                Task.FromResult(OperationResult.AwaitApproval(
                    onApprove:      OperationResult.Next("approved_step", ctx.Context, 30),
                    onReject:       OperationResult.Complete(),
                    timeoutSeconds: 60)))
            .Register("approval_reject", "approved_step", (ctx, ct) =>
            {
                approvedStepRan = true;
                return Task.FromResult(OperationResult.Complete());
            })
            .OnApprovalRequested((request, ct) =>
            {
                capturedRequest = request;
                return Task.CompletedTask;
            });

        var workerTask = worker.RunAsync(cts.Token);

        var (_, tenant) = await CreateIsolatedTenantAsync("approval-reject");
        var workflow = await ControlPlane.CreateWorkflowAsync("approval_reject", tenant.Id,
            workflowConfig: new WorkflowConfig(Version: 1));

        await ControlPlane.ActivateWorkflowAsync(workflow.Id);
        await ControlPlane.InvokeWorkflowAsync(workflow.Id, new RuntimeOverrides(OperationTimeoutSeconds: 30));

        await WaitForLifecycleStateAsync(workflow.Id, LifecycleState.AwaitingApproval, cts.Token);

        Assert.NotNull(capturedRequest);

        // Reject — on_reject = Complete(), so the job just finishes.
        await ControlPlane.RejectWorkflowAsync(workflow.Id, capturedRequest.ApprovalId, cts.Token);
        await WaitForCompletionAsync(workflow.Id, cts.Token);

        Assert.False(approvedStepRan, "approved_step should NOT have run after rejection.");

        cts.Cancel();
        try { await workerTask; } catch { }
    }
}
