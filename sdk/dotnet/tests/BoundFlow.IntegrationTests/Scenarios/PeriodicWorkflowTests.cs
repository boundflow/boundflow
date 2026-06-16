using System.Text.Json.Nodes;
using BoundFlow.ControlPlane;
using BoundFlow.IntegrationTests.Infrastructure;
using BoundFlow.SDK;
using BoundFlow.SDK.Llm;
using Xunit;

namespace BoundFlow.IntegrationTests.Scenarios;

public class PeriodicWorkflowTests : IntegrationTestBase
{
    private const string HaikuModel       = "claude-haiku-4-5-20251001";
    private const int    RepeatEverySeconds = 5;
    private const int    CooldownSeconds    = 8;

    private AgentDefinition Agent() => new(
        Name:         "analyst",
        SystemPrompt: "You are a concise data analyst.",
        Model:        HaikuModel,
        OutputSchema: JsonNode.Parse("{\"summary\":{\"type\":\"string\"}}")
    );

    /// <summary>
    /// Verifies that a workflow with repeat_every_seconds fires automatically at least
    /// twice without any explicit invoke call.
    /// </summary>
    [Fact]
    public async Task PeriodicWorkflowFiresAutomatically()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromMinutes(4));
        var completions = new System.Collections.Concurrent.ConcurrentQueue<DateTimeOffset>();

        var worker = new BoundFlowWorker(WorkerAddress, LlmApiKey, LoggerFactory)
            .RegisterWorkflow("periodic_auto", 1, async (ctx, ct) =>
            {
                ctx.AddLlmContext("task", JsonValue.Create("Summarize in one sentence: BoundFlow schedules agentic workflows."));
                await ctx.RunAgentAsync(Agent(), ct);
                completions.Enqueue(DateTimeOffset.UtcNow);
                return OperationResult.Complete();
            });

        var workerTask = worker.RunAsync(cts.Token);

        var tenant = await CreateIsolatedTenantAsync("periodic-auto");
        var workflow = await ControlPlane.CreateWorkflowAsync("periodic_auto", tenant.Id,
            workflowConfig: new WorkflowConfig(
                Version:             1,
                InvokeTimeoutSeconds: 30,
                RepeatEverySeconds:   RepeatEverySeconds));

        await ControlPlane.ActivateWorkflowAsync(workflow.Id);

        // Wait for at least 2 automatic firings — no explicit invoke.
        while (completions.Count < 2)
            await Task.Delay(500, cts.Token);

        var times = completions.ToArray();
        var gap = times[1] - times[0];
        Assert.True(gap.TotalSeconds >= RepeatEverySeconds,
            $"Expected gap of at least {RepeatEverySeconds}s between firings, got {gap.TotalSeconds:F1}s.");

        cts.Cancel();
        try { await workerTask; } catch { }
        await ControlPlane.DeleteWorkflowAsync(workflow.Id);
    }

    /// <summary>
    /// Verifies that a periodic workflow does not auto-fire while in Cooldown, and resumes
    /// firing after the cooldown expires. The gap between the first and second firing
    /// must be at least the configured cooldown duration.
    /// </summary>
    [Fact]
    public async Task PeriodicWorkflowDoesNotFireDuringCooldown()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromMinutes(5));
        var firings = new System.Collections.Concurrent.ConcurrentQueue<DateTimeOffset>();

        var worker = new BoundFlowWorker(WorkerAddress, LlmApiKey, LoggerFactory)
            .RegisterWorkflow("periodic_cooldown", 1, async (ctx, ct) =>
            {
                firings.Enqueue(DateTimeOffset.UtcNow);
                ctx.AddLlmContext("task", JsonValue.Create("Summarize in one sentence: BoundFlow schedules agentic workflows."));
                await ctx.RunAgentAsync(Agent(), ct);
                return OperationResult.Complete();
            });

        var workerTask = worker.RunAsync(cts.Token);

        var tenant = await CreateIsolatedTenantAsync("periodic-cooldown");
        var workflow = await ControlPlane.CreateWorkflowAsync("periodic_cooldown", tenant.Id,
            workflowConfig: new WorkflowConfig(
                Version:             1,
                InvokeTimeoutSeconds: 30,
                RepeatEverySeconds:   RepeatEverySeconds));

        await ControlPlane.SetWorkflowLifecyclePolicyAsync(
            workflow.Id,
            new WorkflowLifecyclePolicy([
                new WorkflowLifecyclePolicyRule(
                    Metric:    WorkflowMetric.NumLlmCalls,
                    Threshold: 1,
                    Action:    new CooldownAction(Window: 1, CooldownSeconds: CooldownSeconds)
                )
            ]));

        await ControlPlane.ActivateWorkflowAsync(workflow.Id);

        // Wait for the first auto-firing and the resulting cooldown state.
        while (firings.Count < 1)
            await Task.Delay(500, cts.Token);
        await WaitForCompletionAsync(workflow.Id, cts.Token);
        await WaitForWorkflowStateAsync(workflow.Id, WorkflowState.Cooldown, cts.Token);
        var cooldownEnteredAt = DateTimeOffset.UtcNow;

        // Wait for cooldown to expire, then the second auto-firing.
        await WaitForWorkflowStateAsync(workflow.Id, WorkflowState.Active, cts.Token);
        while (firings.Count < 2)
            await Task.Delay(500, cts.Token);

        var secondFiringAt = firings.ToArray()[1];
        var cooldownDuration = secondFiringAt - cooldownEnteredAt;
        Assert.True(cooldownDuration.TotalSeconds >= CooldownSeconds,
            $"Expected second firing at least {CooldownSeconds}s after cooldown start, got {cooldownDuration.TotalSeconds:F1}s.");

        cts.Cancel();
        try { await workerTask; } catch { }
        await ControlPlane.DeleteWorkflowAsync(workflow.Id);
    }
}
