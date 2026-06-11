using System.Collections.Concurrent;
using System.Text.Json.Nodes;
using BoundFlow.ControlPlane;
using BoundFlow.IntegrationTests.Infrastructure;
using BoundFlow.SDK;
using BoundFlow.SDK.Llm;
using Xunit;

namespace BoundFlow.IntegrationTests.Scenarios;

public class AgentLifecyclePolicyTests : IntegrationTestBase
{
    private const string AgentName   = "analyse";
    private const string SonnetModel = "claude-sonnet-4-6";
    private const string HaikuModel  = "claude-haiku-4-5-20251001";

    /// <summary>
    /// Verifies that an agent lifecycle rule fires after the first invocation and
    /// switches the model from Sonnet to Haiku on the second invocation.
    ///
    /// Invoke 1: no prior metrics → rule does not fire → Sonnet is used.
    /// Invoke 2: invoke-1 metrics satisfy the rule (llm_calls >= 1) → Haiku is used.
    /// </summary>
    [Fact]
    public async Task ModelSwitchesToHaikuAfterFirstInvocation()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        var modelResults = new ConcurrentQueue<string>();

        AgentDefinition AnalyseAgent() => new(
            Name:         AgentName,
            SystemPrompt: "You are a concise data analyst.",
            Model:        SonnetModel,
            OutputSchema: JsonNode.Parse("{\"summary\":{\"type\":\"string\"}}")
        );

        var worker = new BoundFlowWorker(WorkerAddress, LlmApiKey, LoggerFactory)
            .RegisterWorkflow("database_agent", 1, async (ctx, ct) =>
            {
                ctx.AddLlmContext("Platform context", JsonValue.Create(
                    "BoundFlow is a tenant-aware control plane for safely scheduling and running agentic workflows."));
                var result = await ctx.RunAgentAsync(AnalyseAgent(), ct);
                modelResults.Enqueue(result.ModelUsed);
                return OperationResult.Complete();
            });

        var workerTask = worker.RunAsync(cts.Token);

        var tenant = await CreateIsolatedTenantAsync("agent-policy");
        var workflow = await ControlPlane.CreateWorkflowAsync("database_agent", tenant.Id,
            workflowConfig: new WorkflowConfig(Version: 1));

        await ControlPlane.SetAgentLifecyclePolicyAsync(
            workflow.Id,
            AgentName,
            new AgentLifecyclePolicy([
                new AgentLifecycleRule(
                    Metric:    AgentMetric.LlmCalls,
                    Operator:  PolicyOperator.GreaterThanOrEqual,
                    Threshold: 1,
                    Window:    1,
                    Action:    new PolicyMutation(PolicyField.Model, HaikuModel)
                )
            ]));

        await ControlPlane.ActivateWorkflowAsync(workflow.Id);

        // Invoke 1 — no prior metrics, rule does not fire → Sonnet
        await ControlPlane.InvokeWorkflowAsync(workflow.Id, new RuntimeOverrides(OperationTimeoutSeconds: 30));
        await WaitForCompletionAsync(workflow.Id, cts.Token);

        Assert.True(modelResults.TryDequeue(out var model1), "Invoke 1 did not produce a result.");
        Assert.Equal(SonnetModel, model1);

        // Invoke 2 — invoke-1 metrics trigger the rule → Haiku
        await ControlPlane.InvokeWorkflowAsync(workflow.Id, new RuntimeOverrides(OperationTimeoutSeconds: 30));
        await WaitForCompletionAsync(workflow.Id, cts.Token);

        Assert.True(modelResults.TryDequeue(out var model2), "Invoke 2 did not produce a result.");
        Assert.Equal(HaikuModel, model2);

        cts.Cancel();
        try { await workerTask; } catch { /* expected on cancellation */ }
    }
}
