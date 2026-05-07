using System.Text.Json.Nodes;
using BoundFlow.SDK;
using BoundFlow.SDK.Llm;
using BoundFlow.ControlPlane;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Console;

// dotnet run --project src/BoundFlow.TestApp

// ── Config ────────────────────────────────────────────────────────────────────
// Server must be running locally: ./bin/convergeplane -mode=server
// Worker must be running locally: ./bin/convergeplane -mode=worker
// Scheduler must be running locally: ./bin/convergeplane -mode=scheduler
//
// For deterministic-only tests (no agent steps) any string works as the API key.
// For agent step tests set ANTHROPIC_API_KEY in your environment.
var workerAddress = "http://localhost:50052";
var serverAddress = "http://localhost:50051";
var llmApiKey = Environment.GetEnvironmentVariable("ANTHROPIC_API_KEY") ?? "test-key";

using var loggerFactory = LoggerFactory.Create(b => b.AddConsole().SetMinimumLevel(LogLevel.Debug));

var cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) => { e.Cancel = true; cts.Cancel(); };

// ── Worker — handle the operations dispatched for this resource type ──────────
var worker = new BoundFlowWorker(workerAddress, llmApiKey, loggerFactory)

    .Register("database_agent", "create_entry", async (ctx, ct) =>
    {
        Console.WriteLine($"[database_agent/create_entry] context: {ctx.Context}");
        await Task.Delay(500, ct);
        return OperationResult.Next("analyse_entry", ctx.Context, 60);
    })

    .Register("database_agent", "reconcile_entry", async (ctx, ct) =>
    {
        Console.WriteLine($"[database_agent/reconcile_entry] context: {ctx.Context}");
        await Task.Delay(500, ct);
        return OperationResult.Complete();
    })

    .Register("database_agent", "delete_entry", async (ctx, ct) =>
    {
        Console.WriteLine($"[database_agent/delete_entry] context: {ctx.Context}");
        await Task.Delay(300, ct);
        return OperationResult.Complete();
    })

    // Agent step — requires a real ANTHROPIC_API_KEY.
    .Register("database_agent", "analyse_entry", async (ctx, ct) =>
    {
        var boundFlowInfo = "BoundFlow is a tenant-aware control plane for safely scheduling, running, and auditing long-lived resource or agent workflows with policies, retries, state tracking, and customer-owned execution logic.";
        ctx.AddLlmContext("This is information about a platform called boundflow", JsonValue.Create(boundFlowInfo));

        var result = await ctx.RunAgentStepAsync(new AgentStepConfig(
            Objective: "Summarise the context data in one sentence.",
            SystemPrompt: "You are a concise data analyst.",
            Policy: new AgentPolicy(MaxLlmCalls: 3),
            OutputSchema: JsonNode.Parse("{\"summary\":{\"type\":\"string\"}}")
        ), ct);

        Console.WriteLine($"[database_agent/analyse_entry] LLM result: {result.Output}");
        return OperationResult.Complete();
    });

Console.WriteLine("BoundFlow test worker running. Press Ctrl+C to stop.");
Console.WriteLine($"  Worker : {workerAddress}");
Console.WriteLine($"  Server : {serverAddress}");
Console.WriteLine($"  LLM key: {(llmApiKey == "test-key" ? "none (deterministic only)" : "set")}");
Console.WriteLine();

var workerTask = worker.RunAsync(cts.Token);

// ── Control plane — create the resource to trigger the workflow ───────────────
var controlPlane = new ControlPlaneClient(serverAddress);
var tenantGroup = await controlPlane.CreateTenantGroupAsync("test-group");
var tenant = await controlPlane.CreateTenantAsync("test-tenant", tenantGroup.Id);
var resource = await controlPlane.CreateResourceAsync(
    resourceType: "database_agent",
    tenantId: tenant.Id,
    initialState: JsonNode.Parse("{\"sku\": \"standard\"}")!,
    operationTimeoutSeconds: 30);

ResourceState state;
do
{
    await Task.Delay(500);
    state = await controlPlane.GetResourceStateAsync(resource.Id);
    Console.WriteLine($"Current resource state: {state.LifecycleState}");
}
while (state.LifecycleState == LifecycleState.Creating);
if (state.LifecycleState != LifecycleState.Active)
{
    Console.WriteLine("Resource creation failure!");
}

Console.WriteLine($"lifecycle: {state.LifecycleState}");

await workerTask;