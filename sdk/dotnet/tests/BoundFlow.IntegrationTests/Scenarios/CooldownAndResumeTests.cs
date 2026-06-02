using System.Text.Json.Nodes;
using BoundFlow.ControlPlane;
using BoundFlow.IntegrationTests.Infrastructure;
using BoundFlow.SDK;
using BoundFlow.SDK.Llm;
using Xunit;

namespace BoundFlow.IntegrationTests.Scenarios;

public class CooldownAndResumeTests : IntegrationTestBase
{
    private const string HaikuModel = "claude-haiku-4-5-20251001";
    private const int CooldownSeconds = 8;

    private AgentDefinition Agent() => new(
        Name:         "analyst",
        SystemPrompt: "You are a concise data analyst.",
        Model:        HaikuModel,
        OutputSchema: JsonNode.Parse("{\"summary\":{\"type\":\"string\"}}")
    );

    private WorkflowLifecyclePolicy CooldownPolicy() => new([
        new WorkflowLifecyclePolicyRule(
            Metric:    WorkflowMetric.NumLlmCalls,
            Threshold: 1,
            Action:    new CooldownAction(Window: 1, CooldownSeconds: CooldownSeconds)
        )
    ]);

    /// <summary>
    /// Verifies that a workflow in Cooldown automatically transitions back to Active
    /// once the cooldown window expires and the lifecycle resolver ticks.
    /// </summary>
    [Fact]
    public async Task WorkflowResumesAfterCooldownExpires()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromMinutes(3));

        var worker = new BoundFlowWorker(WorkerAddress, LlmApiKey, LoggerFactory)
            .RegisterWorkflow("cooldown_resume", 1, async (ctx, ct) =>
            {
                ctx.AddLlmContext("task", JsonValue.Create("Summarize in one sentence: BoundFlow schedules agentic workflows."));
                await ctx.RunAgentAsync(Agent(), ct);
                return OperationResult.Complete();
            });

        var workerTask = worker.RunAsync(cts.Token);

        var (_, tenant) = await CreateIsolatedTenantAsync("cooldown-resume");
        var workflow = await ControlPlane.CreateWorkflowAsync("cooldown_resume", tenant.Id,
            workflowConfig: new WorkflowConfig(Version: 1));

        await ControlPlane.SetWorkflowLifecyclePolicyAsync(workflow.Id, CooldownPolicy());
        await ControlPlane.ActivateWorkflowAsync(workflow.Id);

        // Invoke — rule fires on completion, workflow enters Cooldown.
        await ControlPlane.InvokeWorkflowAsync(workflow.Id, new RuntimeOverrides(OperationTimeoutSeconds: 30));
        await WaitForCompletionAsync(workflow.Id, cts.Token);
        await WaitForWorkflowStateAsync(workflow.Id, WorkflowState.Cooldown, cts.Token);

        // Wait for the lifecycle resolver to expire the cooldown and flip back to Active.
        var cooldownEnteredAt = DateTimeOffset.UtcNow;
        await WaitForWorkflowStateAsync(workflow.Id, WorkflowState.Active, cts.Token);
        var elapsed = DateTimeOffset.UtcNow - cooldownEnteredAt;

        Assert.Equal(WorkflowState.Active, await ControlPlane.GetWorkflowStateAsync(workflow.Id));
        Assert.True(elapsed.TotalSeconds >= CooldownSeconds,
            $"Cooldown lasted {elapsed.TotalSeconds:F1}s but was configured for {CooldownSeconds}s.");

        cts.Cancel();
        try { await workerTask; } catch { }
    }

    /// <summary>
    /// Verifies that invoking a workflow in Cooldown is rejected with FailedPrecondition,
    /// and that invocation succeeds once the cooldown expires.
    /// </summary>
    [Fact]
    public async Task InvokeWhileInCooldownIsRejectedThenSucceedsAfterResume()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromMinutes(3));
        var completed = new System.Collections.Concurrent.ConcurrentQueue<bool>();

        var worker = new BoundFlowWorker(WorkerAddress, LlmApiKey, LoggerFactory)
            .RegisterWorkflow("cooldown_invoke", 1, async (ctx, ct) =>
            {
                ctx.AddLlmContext("task", JsonValue.Create("Summarize in one sentence: BoundFlow schedules agentic workflows."));
                await ctx.RunAgentAsync(Agent(), ct);
                completed.Enqueue(true);
                return OperationResult.Complete();
            });

        var workerTask = worker.RunAsync(cts.Token);

        var (_, tenant) = await CreateIsolatedTenantAsync("cooldown-invoke");
        var workflow = await ControlPlane.CreateWorkflowAsync("cooldown_invoke", tenant.Id,
            workflowConfig: new WorkflowConfig(Version: 1));

        await ControlPlane.SetWorkflowLifecyclePolicyAsync(workflow.Id, CooldownPolicy());
        await ControlPlane.ActivateWorkflowAsync(workflow.Id);

        // Invoke 1 — triggers cooldown rule.
        await ControlPlane.InvokeWorkflowAsync(workflow.Id, new RuntimeOverrides(OperationTimeoutSeconds: 30));
        await WaitForCompletionAsync(workflow.Id, cts.Token);
        await WaitForWorkflowStateAsync(workflow.Id, WorkflowState.Cooldown, cts.Token);

        // Invoke while in Cooldown — must be rejected immediately.
        var ex = await Assert.ThrowsAsync<Grpc.Core.RpcException>(() =>
            ControlPlane.InvokeWorkflowAsync(workflow.Id, new RuntimeOverrides(OperationTimeoutSeconds: 30)));
        Assert.Equal(Grpc.Core.StatusCode.FailedPrecondition, ex.StatusCode);

        // Wait for cooldown to expire and workflow to return to Active.
        await WaitForWorkflowStateAsync(workflow.Id, WorkflowState.Active, cts.Token);

        // Invoke 2 — should now succeed.
        await ControlPlane.InvokeWorkflowAsync(workflow.Id, new RuntimeOverrides(OperationTimeoutSeconds: 30));
        await WaitForCompletionAsync(workflow.Id, cts.Token);

        Assert.True(completed.Count == 2, $"Expected 2 completions, got {completed.Count}.");

        cts.Cancel();
        try { await workerTask; } catch { }
    }
}
