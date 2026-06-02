using System.Text.Json.Nodes;
using BoundFlow.ControlPlane;
using BoundFlow.IntegrationTests.Infrastructure;
using BoundFlow.SDK;
using BoundFlow.SDK.Llm;
using Xunit;

namespace BoundFlow.IntegrationTests.Scenarios;

public class WorkflowLifecyclePolicyTests : IntegrationTestBase
{
    private const string HaikuModel = "claude-haiku-4-5-20251001";

    /// <summary>
    /// Verifies that a workflow-level cooldown rule fires after the first invocation
    /// makes at least one LLM call, and that the workflow transitions to Cooldown.
    /// </summary>
    [Fact]
    public async Task WorkflowEntersCooldownAfterLlmCallThresholdExceeded()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromMinutes(2));

        AgentDefinition Agent() => new(
            Name:         "analyst",
            SystemPrompt: "You are a concise data analyst.",
            Model:        HaikuModel,
            OutputSchema: JsonNode.Parse("{\"summary\":{\"type\":\"string\"}}")
        );

        var worker = new BoundFlowWorker(WorkerAddress, LlmApiKey, LoggerFactory)
            .RegisterWorkflow("cooldown_test", 1, async (ctx, ct) =>
            {
                ctx.AddLlmContext("task", JsonValue.Create("Summarize in one sentence: BoundFlow schedules agentic workflows."));
                await ctx.RunAgentAsync(Agent(), ct);
                return OperationResult.Complete();
            });

        var workerTask = worker.RunAsync(cts.Token);

        var (_, tenant) = await CreateIsolatedTenantAsync("workflow-policy");
        var workflow = await ControlPlane.CreateWorkflowAsync("cooldown_test", tenant.Id,
            workflowConfig: new WorkflowConfig(Version: 1));

        await ControlPlane.SetWorkflowLifecyclePolicyAsync(
            workflow.Id,
            new WorkflowLifecyclePolicy([
                new WorkflowLifecyclePolicyRule(
                    Metric:    WorkflowMetric.NumLlmCalls,
                    Threshold: 1,
                    Action:    new CooldownAction(Window: 1, CooldownSeconds: 10)
                )
            ]));

        await ControlPlane.ActivateWorkflowAsync(workflow.Id);

        await ControlPlane.InvokeWorkflowAsync(workflow.Id, new RuntimeOverrides(OperationTimeoutSeconds: 30));
        await WaitForCompletionAsync(workflow.Id, cts.Token);

        // Lifecycle resolver fires synchronously in the completion path — wait briefly
        // for the workflow_state to transition.
        var state = await WaitForWorkflowStateAsync(workflow.Id, WorkflowState.Cooldown, cts.Token);
        Assert.Equal(WorkflowState.Cooldown, state);

        cts.Cancel();
        try { await workerTask; } catch { }
    }

    /// <summary>
    /// Verifies that a set_version rule fires when the version-total LLM call count
    /// exceeds the threshold, and that subsequent invocations use the new version.
    /// </summary>
    [Fact]
    public async Task WorkflowSwitchesToNewVersionAfterTotalLlmCallThresholdExceeded()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromMinutes(3));
        var versionsRun = new System.Collections.Concurrent.ConcurrentQueue<int>();

        AgentDefinition Agent() => new(
            Name:         "analyst",
            SystemPrompt: "You are a concise data analyst.",
            Model:        HaikuModel,
            OutputSchema: JsonNode.Parse("{\"summary\":{\"type\":\"string\"}}")
        );

        var worker = new BoundFlowWorker(WorkerAddress, LlmApiKey, LoggerFactory)
            .RegisterWorkflow("version_test", 1, async (ctx, ct) =>
            {
                ctx.AddLlmContext("task", JsonValue.Create("Summarize in one sentence: BoundFlow schedules agentic workflows."));
                await ctx.RunAgentAsync(Agent(), ct);
                versionsRun.Enqueue(ctx.WorkflowVersion);
                return OperationResult.Complete();
            })
            .RegisterWorkflow("version_test", 2, (ctx, ct) =>
            {
                versionsRun.Enqueue(ctx.WorkflowVersion);
                return Task.FromResult(OperationResult.Complete());
            });

        var workerTask = worker.RunAsync(cts.Token);

        var (_, tenant) = await CreateIsolatedTenantAsync("version-rollback");
        var workflow = await ControlPlane.CreateWorkflowAsync("version_test", tenant.Id,
            workflowConfig: new WorkflowConfig(Version: 1));

        // Switch to version 2 once total LLM calls across all runs reaches 1.
        await ControlPlane.SetWorkflowLifecyclePolicyAsync(
            workflow.Id,
            new WorkflowLifecyclePolicy([
                new WorkflowLifecyclePolicyRule(
                    Metric:    WorkflowMetric.NumLlmCalls,
                    Threshold: 1,
                    Action:    new SetVersionAction(TargetVersion: 2)
                )
            ]));

        await ControlPlane.ActivateWorkflowAsync(workflow.Id);

        // Invoke 1 — runs version 1, makes an LLM call, rule fires → current_workflow_version = 2.
        await ControlPlane.InvokeWorkflowAsync(workflow.Id, new RuntimeOverrides(OperationTimeoutSeconds: 30));
        await WaitForCompletionAsync(workflow.Id, cts.Token);

        Assert.True(versionsRun.TryDequeue(out var v1), "Invoke 1 did not complete.");
        Assert.Equal(1, v1);

        // Invoke 2 — scheduler reads current_workflow_version = 2, dispatches to version 2 handler.
        await ControlPlane.InvokeWorkflowAsync(workflow.Id, new RuntimeOverrides(OperationTimeoutSeconds: 30));
        await WaitForCompletionAsync(workflow.Id, cts.Token);

        Assert.True(versionsRun.TryDequeue(out var v2), "Invoke 2 did not complete.");
        Assert.Equal(2, v2);

        cts.Cancel();
        try { await workerTask; } catch { }
    }

    /// <summary>
    /// Verifies that a pause rule fires and the workflow enters Paused state,
    /// and that a queued invocation does not execute until the workflow is explicitly activated.
    /// </summary>
    [Fact]
    public async Task WorkflowPausesAndDoesNotScheduleUntilActivated()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromMinutes(3));
        var completions = new System.Collections.Concurrent.ConcurrentQueue<int>();
        var invocationCount = 0;

        AgentDefinition Agent() => new(
            Name:         "analyst",
            SystemPrompt: "You are a concise data analyst.",
            Model:        HaikuModel,
            OutputSchema: JsonNode.Parse("{\"summary\":{\"type\":\"string\"}}")
        );

        var worker = new BoundFlowWorker(WorkerAddress, LlmApiKey, LoggerFactory)
            .RegisterWorkflow("pause_test", 1, async (ctx, ct) =>
            {
                var n = System.Threading.Interlocked.Increment(ref invocationCount);
                await ctx.RunAgentAsync(Agent(), ct);
                completions.Enqueue(n);
                return OperationResult.Complete();
            });

        var workerTask = worker.RunAsync(cts.Token);

        var (_, tenant) = await CreateIsolatedTenantAsync("workflow-pause");
        var workflow = await ControlPlane.CreateWorkflowAsync("pause_test", tenant.Id,
            workflowConfig: new WorkflowConfig(Version: 1));

        await ControlPlane.SetWorkflowLifecyclePolicyAsync(
            workflow.Id,
            new WorkflowLifecyclePolicy([
                new WorkflowLifecyclePolicyRule(
                    Metric:    WorkflowMetric.NumLlmCalls,
                    Threshold: 1,
                    Action:    new PauseAction(Window: 1)
                )
            ]));

        await ControlPlane.ActivateWorkflowAsync(workflow.Id);

        // Invoke 1 — pause rule fires on completion
        await ControlPlane.InvokeWorkflowAsync(workflow.Id, new RuntimeOverrides(OperationTimeoutSeconds: 30));
        await WaitForCompletionAsync(workflow.Id, cts.Token);
        await WaitForWorkflowStateAsync(workflow.Id, WorkflowState.Paused, cts.Token);

        // Invoke 2 — should be rejected immediately with FailedPrecondition.
        var ex = await Assert.ThrowsAsync<Grpc.Core.RpcException>(() =>
            ControlPlane.InvokeWorkflowAsync(workflow.Id, new RuntimeOverrides(OperationTimeoutSeconds: 30)));
        Assert.Equal(Grpc.Core.StatusCode.FailedPrecondition, ex.StatusCode);

        // Activate — invoke should now succeed and complete.
        await ControlPlane.ActivateWorkflowAsync(workflow.Id);
        await ControlPlane.InvokeWorkflowAsync(workflow.Id, new RuntimeOverrides(OperationTimeoutSeconds: 30));
        await WaitForCompletionAsync(workflow.Id, cts.Token);

        Assert.True(completions.Any(n => n == 2), "Invoke 2 should have completed after activation.");
    }
}
