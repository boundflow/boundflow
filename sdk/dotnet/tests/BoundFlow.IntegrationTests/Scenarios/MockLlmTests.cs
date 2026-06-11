using System.Text.Json.Nodes;
using BoundFlow.ControlPlane;
using BoundFlow.IntegrationTests.Infrastructure;
using BoundFlow.SDK;
using BoundFlow.SDK.Llm;
using Xunit;

namespace BoundFlow.IntegrationTests.Scenarios;

public class MockLlmTests : IntegrationTestBase
{
    private const string AgentName = "mocked";

    /// <summary>
    /// Drives the orchestrator with a scripted mock LLM (no API key): the model "calls"
    /// the ping tool on the first three turns then submits. With a per-tool cap of 1, the
    /// customer's handler must only fire once — proving both the mock and enforcement.
    /// </summary>
    [Fact]
    public async Task MockLlm_DrivesToolCalls_AndPerToolLimitHolds()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromMinutes(1));
        var pingHandlerCalls = 0;

        // Scripted model: call ping for the first 3 turns, then submit_result.
        var mock = new MockLlmClient(ctx =>
            ctx.TurnIndex < 3
                ? new MockTurn([new MockToolCall("ping")], InputTokens: 100, OutputTokens: 50)
                : new MockTurn([new MockToolCall("submit_result", JsonNode.Parse("{\"done\":true}"))]));

        AgentDefinition Agent() => new(
            Name:         AgentName,
            SystemPrompt: "mock agent",
            Model:        "mock-model",
            AllowedCallbacks: [
                new AllowedCallback(
                    Name:        "ping",
                    Description: "ping",
                    Handler:     (input, ct) =>
                    {
                        System.Threading.Interlocked.Increment(ref pingHandlerCalls);
                        return Task.FromResult<JsonNode?>(JsonValue.Create("pong"));
                    })
            ],
            OutputSchema: JsonNode.Parse("{\"done\":{\"type\":\"boolean\"}}")
        );

        var worker = new BoundFlowWorker(WorkerAddress, mock, LoggerFactory)
            .RegisterWorkflow("mock_tool_limit", 1, async (ctx, ct) =>
            {
                await ctx.RunAgentAsync(Agent(), ct);
                return OperationResult.Complete();
            });

        var workerTask = worker.RunAsync(cts.Token);

        var tenant = await CreateIsolatedTenantAsync("mock-tool-limit");
        var workflow = await ControlPlane.CreateWorkflowAsync("mock_tool_limit", tenant.Id,
            workflowConfig: new WorkflowConfig(Version: 1));

        await ControlPlane.SetAgentRuntimePolicyAsync(
            workflow.Id,
            AgentName,
            new AgentRuntimePolicy(
                MaxLlmCalls: 8,
                ToolCallLimits: [new ToolCallLimit("ping", 1)]
            ));

        await ControlPlane.ActivateWorkflowAsync(workflow.Id);
        await ControlPlane.InvokeWorkflowAsync(workflow.Id, new RuntimeOverrides(OperationTimeoutSeconds: 30));
        await WaitForCompletionAsync(workflow.Id, cts.Token);

        Assert.Equal(1, pingHandlerCalls);

        cts.Cancel();
        try { await workerTask; } catch { }
    }
}
