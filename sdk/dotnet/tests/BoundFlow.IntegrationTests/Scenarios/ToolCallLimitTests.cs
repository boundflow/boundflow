using System.Text.Json.Nodes;
using BoundFlow.ControlPlane;
using BoundFlow.IntegrationTests.Infrastructure;
using BoundFlow.SDK;
using BoundFlow.SDK.Llm;
using Xunit;

namespace BoundFlow.IntegrationTests.Scenarios;

public class ToolCallLimitTests : IntegrationTestBase
{
    private const string HaikuModel = "claude-haiku-4-5-20251001";
    private const string AgentName  = "limited";

    /// <summary>
    /// Verifies that a per-tool call limit is enforced: the LLM is instructed to call a
    /// tool several times, but the runtime policy caps it at 1, so the customer's tool
    /// handler is invoked at most once regardless of how many times the model tries.
    /// </summary>
    [Fact]
    public async Task PerToolLimit_CapsHandlerInvocations()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        var pingHandlerCalls = 0;

        AgentDefinition Agent() => new(
            Name:         AgentName,
            SystemPrompt: "You are a test agent. You MUST call the `ping` tool 4 separate times, " +
                          "one at a time, before you finish. After attempting all 4 calls, call submit_result.",
            Model:        HaikuModel,
            AllowedCallbacks: [
                new AllowedCallback(
                    Name:        "ping",
                    Description: "A no-op ping tool. Call it to register a ping.",
                    Handler:     (input, ct) =>
                    {
                        System.Threading.Interlocked.Increment(ref pingHandlerCalls);
                        return Task.FromResult<JsonNode?>(JsonValue.Create("pong"));
                    })
            ],
            OutputSchema: JsonNode.Parse("{\"done\":{\"type\":\"boolean\"}}")
        );

        var worker = new BoundFlowWorker(WorkerAddress, LlmApiKey, LoggerFactory)
            .RegisterWorkflow("tool_limit_test", 1, async (ctx, ct) =>
            {
                await ctx.RunAgentAsync(Agent(), ct);
                return OperationResult.Complete();
            });

        var workerTask = worker.RunAsync(cts.Token);

        var (_, tenant) = await CreateIsolatedTenantAsync("tool-limit");
        var workflow = await ControlPlane.CreateWorkflowAsync("tool_limit_test", tenant.Id,
            workflowConfig: new WorkflowConfig(Version: 1));

        // Cap ping at 1 call; give enough LLM turns to attempt several.
        await ControlPlane.SetAgentRuntimePolicyAsync(
            workflow.Id,
            AgentName,
            new AgentRuntimePolicy(
                MaxLlmCalls: 8,
                ToolCallLimits: [new ToolCallLimit("ping", 1)]
            ));

        await ControlPlane.ActivateWorkflowAsync(workflow.Id);

        await ControlPlane.InvokeWorkflowAsync(workflow.Id, new RuntimeOverrides(OperationTimeoutSeconds: 60));
        await WaitForCompletionAsync(workflow.Id, cts.Token);

        // The model tried to call ping 4 times, but the cap means the handler ran at most once.
        Assert.True(pingHandlerCalls >= 1, "ping handler should have run at least once.");
        Assert.True(pingHandlerCalls <= 1, $"ping handler should have been capped at 1, but ran {pingHandlerCalls} times.");

        cts.Cancel();
        try { await workerTask; } catch { }
    }
}
