using System.Text.Json.Nodes;
using BoundFlow.SDK;
using BoundFlow.SDK.Llm;
using BoundFlow.ControlPlane;
using Microsoft.Extensions.Logging;

// dotnet run --project src/BoundFlow.TestApp
//
// Server must be running locally: ./bin/convergeplane -mode=server
// Worker must be running locally: ./bin/convergeplane -mode=worker
// Scheduler must be running locally: ./bin/convergeplane -mode=scheduler
//
// Requires a real ANTHROPIC_API_KEY — the analyse step calls the Claude API.

var workerAddress = "http://localhost:50052";
var serverAddress = "http://localhost:50051";
var llmApiKey = Environment.GetEnvironmentVariable("ANTHROPIC_API_KEY")
    ?? throw new InvalidOperationException("ANTHROPIC_API_KEY is required for this test.");

const string AgentName  = "analyse";
const string SonnetModel = "claude-sonnet-4-6";
const string HaikuModel  = "claude-haiku-4-5-20251001";

using var loggerFactory = LoggerFactory.Create(b => b.AddConsole().SetMinimumLevel(LogLevel.Warning));

var cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) => { e.Cancel = true; cts.Cancel(); };

// Shared agent definition — model is the AgentDefinition default; lifecycle rule may override it.
AgentDefinition AnalyseAgent() => new AgentDefinition(
    Name:         AgentName,
    SystemPrompt: "You are a concise data analyst.",
    Model:        SonnetModel,
    OutputSchema: JsonNode.Parse("{\"summary\":{\"type\":\"string\"}}")
);

// ── Worker ───────────────────────────────────────────────────────────────────
var worker = new BoundFlowWorker(workerAddress, llmApiKey, loggerFactory)

    .Register("database_agent", "create_entry", async (ctx, ct) =>
    {
        Console.WriteLine("[create_entry] running");
        await Task.Delay(200, ct);
        return OperationResult.Next("analyse_entry", ctx.Context, 60);
    })

    .Register("database_agent", "analyse_entry", async (ctx, ct) =>
    {
        var boundFlowInfo = "BoundFlow is a tenant-aware control plane for safely scheduling and running agentic workflows.";
        ctx.AddLlmContext("Platform context", JsonValue.Create(boundFlowInfo));

        var result = await ctx.RunAgentAsync(AnalyseAgent(), ct);

        Console.WriteLine($"[analyse_entry] model_used={result.ModelUsed}  llm_calls={result.LlmCallsUsed}");
        return OperationResult.Complete();
    })

    .Register("database_agent", "reconcile_entry", async (ctx, ct) =>
    {
        Console.WriteLine("[reconcile_entry] running agent step");

        var boundFlowInfo = "BoundFlow is a tenant-aware control plane for safely scheduling and running agentic workflows.";
        ctx.AddLlmContext("Platform context", JsonValue.Create(boundFlowInfo));

        var result = await ctx.RunAgentAsync(AnalyseAgent(), ct);

        Console.WriteLine($"[reconcile_entry] model_used={result.ModelUsed}  llm_calls={result.LlmCallsUsed}");
        return OperationResult.Complete();
    })

    .Register("database_agent", "delete_entry", async (ctx, ct) =>
    {
        await Task.Delay(200, ct);
        return OperationResult.Complete();
    });

Console.WriteLine($"Worker  : {workerAddress}");
Console.WriteLine($"Server  : {serverAddress}");
Console.WriteLine();

var workerTask = worker.RunAsync(cts.Token);

// ── Control plane ────────────────────────────────────────────────────────────
var cp = new ControlPlaneClient(serverAddress);

var tenantGroup = await cp.CreateTenantGroupAsync("test-group");
var tenant      = await cp.CreateTenantAsync("test-tenant", tenantGroup.Id);
var resource    = await cp.CreateResourceAsync(
    resourceType:            "database_agent",
    tenantId:                tenant.Id,
    initialState:            JsonNode.Parse("{\"sku\": \"standard\"}")!,
    operationTimeoutSeconds: 60);

Console.WriteLine($"Resource {resource.Id} created — waiting for Active...");

// ── Run 1: create → analyse_entry (no policies set yet, runs with sonnet default) ──
ResourceState state;
do
{
    await Task.Delay(500);
    state = await cp.GetResourceStateAsync(resource.Id);
}
while (state.LifecycleState == LifecycleState.Creating);

if (state.LifecycleState != LifecycleState.Active)
{
    Console.WriteLine($"Resource failed to become active: {state.LifecycleState}");
    return;
}

Console.WriteLine($"\nRun 1 complete. Resource is Active.");
Console.WriteLine($"Expected: model_used={SonnetModel} (no lifecycle rule has fired yet)\n");

// ── Set policies now that we have a resource_instance_id and run 1 metrics ──
Console.WriteLine("Setting lifecycle policy: if llm_calls >= 1 in last invocation → switch to haiku...");

await cp.SetAgentLifecyclePolicyAsync(
    resource.Id,
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

Console.WriteLine("Lifecycle policy set.\n");

// ── Run 2: reconcile → analyse_entry with lifecycle rule active ──────────────
Console.WriteLine("Triggering reconcile (run 2)...");

await cp.ReconcileResourceAsync(
    resource.Id,
    goalState: JsonNode.Parse("{\"sku\": \"standard\"}")!,
    operationTimeoutSeconds: 60);

do
{
    await Task.Delay(500);
    state = await cp.GetResourceStateAsync(resource.Id);
}
while (state.LifecycleState == LifecycleState.Reconciling);

Console.WriteLine($"\nRun 2 complete.");
Console.WriteLine($"Expected: model_used={HaikuModel} (lifecycle rule fired because run 1 had llm_calls >= 1)");

cts.Cancel();
try { await workerTask; } catch (Exception) { /* expected on cancellation */ }