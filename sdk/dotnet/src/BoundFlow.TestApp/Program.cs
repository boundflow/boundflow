using System.Text.Json.Nodes;
using BoundFlow.SDK;
using BoundFlow.SDK.Llm;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Console;

// ── Config ────────────────────────────────────────────────────────────────────
// Server must be running locally: ./bin/convergeplane -mode=server
// Worker must be running locally: ./bin/convergeplane -mode=worker
// Scheduler must be running locally: ./bin/convergeplane -mode=scheduler
//
// For deterministic-only tests (no agent steps) any string works as the API key.
// For agent step tests set ANTHROPIC_API_KEY in your environment.
var serverAddress = "http://localhost:50052";
var llmApiKey = Environment.GetEnvironmentVariable("ANTHROPIC_API_KEY") ?? "test-key";

using var loggerFactory = LoggerFactory.Create(b => b.AddConsole().SetMinimumLevel(LogLevel.Debug));

var cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) => { e.Cancel = true; cts.Cancel(); };

// ── Worker setup ──────────────────────────────────────────────────────────────
var worker = new BoundFlowWorker(serverAddress, llmApiKey, loggerFactory)

    // Simple deterministic step — just logs context and completes.
    .Register("create_entry", async (ctx, ct) =>
    {
        Console.WriteLine($"[create_entry] context: {ctx.Context}");
        await Task.Delay(500, ct); // simulate work
        return OperationResult.Complete();
    })

    // Multi-step: first step advances to a second step.
    .Register("provision_entry", async (ctx, ct) =>
    {
        Console.WriteLine($"[provision_entry] starting provisioning");
        await Task.Delay(500, ct);
        return OperationResult.Next(
            "provision_verify",
            JsonNode.Parse("{\"provisioned\": true}")!,
            timeoutSeconds: 30);
    })

    .Register("provision_verify", async (ctx, ct) =>
    {
        var provisioned = ctx.Context?["provisioned"]?.GetValue<bool>() ?? false;
        Console.WriteLine($"[provision_verify] provisioned={provisioned}");
        await Task.Delay(300, ct);
        return OperationResult.Complete();
    })

    // Agent step — requires a real ANTHROPIC_API_KEY.
    // Uncomment and set the env var to test LLM functionality.
    //
    // .Register("analyse_entry", async (ctx, ct) =>
    // {
    //     var result = await ctx.RunAgentStepAsync(new AgentStepConfig(
    //         Objective: "Summarise the context data in one sentence.",
    //         SystemPrompt: "You are a concise data analyst.",
    //         Policy: new AgentPolicy(MaxLlmCalls: 3),
    //         OutputSchema: JsonNode.Parse("{\"summary\":{\"type\":\"string\"}}")
    //     ), ct);
    //
    //     Console.WriteLine($"[analyse_entry] LLM result: {result.Output}");
    //     return OperationResult.Complete();
    // })
    ;

Console.WriteLine("BoundFlow test worker running. Press Ctrl+C to stop.");
Console.WriteLine($"  Server : {serverAddress}");
Console.WriteLine($"  LLM key: {(llmApiKey == "test-key" ? "none (deterministic only)" : "set")}");
Console.WriteLine();

await worker.RunAsync(cts.Token);
