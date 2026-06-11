using System.Collections.Concurrent;
using System.Text.Json.Nodes;
using BoundFlow.ControlPlane;
using BoundFlow.SDK;
using BoundFlow.SDK.Llm;
using Microsoft.Extensions.Logging;

// ─────────────────────────────────────────────────────────────────────────────
// BoundFlow demo — Acme's three agentic workflows under governance.
//
// Prereqs: the BoundFlow stack must be running locally (./localrun/dev.sh):
//   server   :50051
//   worker   :50052
//   scheduler
//
// No ANTHROPIC_API_KEY needed — all runs use a scripted mock LLM so cost,
// LLM-call counts, tool calls, and failures are deterministic.
// ─────────────────────────────────────────────────────────────────────────────

const string WorkerAddress = "http://localhost:50052";
const string ServerAddress = "http://localhost:50051";

const string SonnetModel = "claude-sonnet-4-6";
const string HaikuModel  = "claude-haiku-4-5-20251001";
const string OpusModel   = "claude-opus-4-8";

using var loggerFactory = LoggerFactory.Create(b => b.AddConsole().SetMinimumLevel(LogLevel.Error));

var cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) => { e.Cancel = true; cts.Cancel(); };

var state = new DemoState();
var approvals = new ConcurrentDictionary<string, ApprovalRequest>();

// ── Mock LLM: scripted per agent (branch on the system prompt tag) ───────────
var mock = new MockLlmClient(ctx =>
{
    var sp = ctx.SystemPrompt;

    if (sp.Contains("[ticket-triage]"))
        return ctx.TurnIndex == 0
            ? Turn(300, 150, "get_ticket", "search_kb")
            : Submit();

    if (sp.Contains("[order-analyze]"))
    {
        if (ctx.TurnIndex == 0)
        {
            // Cost spike: report a large token bill so the cost metric jumps.
            if (state.CostSpike) return Turn(500_000, 200_000, "get_order_status", "get_queue_depth");
            // Loop: agent retries the same tool repeatedly — triggers CallsPerTool lifecycle rule.
            if (state.Looping) return Turn(400, 200,
                "retry_fulfillment_job", "retry_fulfillment_job",
                "retry_fulfillment_job", "retry_fulfillment_job");
            return Turn(400, 200, "get_order_status", "get_queue_depth");
        }
        return Submit();
    }

    if (sp.Contains("[order-analyze-v2]"))
        return ctx.TurnIndex == 0 ? Turn(400, 200, "get_order_status", "smart_retry_v2") : Submit();

    if (sp.Contains("[order-rollback]"))
        return ctx.TurnIndex == 0 ? Turn(300, 150, "rollback_fulfillment_config") : Submit();

    if (sp.Contains("[incident-diagnosis]"))
        return ctx.TurnIndex == 0
            ? Turn(400, 200, "get_service_health", "get_recent_deployments", "open_incident_ticket")
            : Submit();

    return Submit();
});

// ── Worker: register Acme's three workflows ──────────────────────────────────
var worker = new BoundFlowWorker(WorkerAddress, mock, loggerFactory);

// Support: ticket-triage — read-only analysis, then a gated customer-facing send.
worker.RegisterWorkflow("ticket-triage", 1, async (octx, ct) =>
{
    await octx.RunAgentAsync(TicketTriageAgent(), ct);
    return OperationResult.AwaitApproval(
        onApprove:      OperationResult.Next("send_response", octx.Context, 30),
        onReject:       OperationResult.Complete(),
        timeoutSeconds: 120,
        justification:  "Send suggested response to the customer?");
});
worker.Register("ticket-triage", "send_response", (octx, ct) =>
{
    Console.WriteLine("      → suggest_response sent to customer.");
    return Task.FromResult(OperationResult.Complete());
});

// Operations: order-remediation v1 — uses proven retry_fulfillment_job tool, rollback is gated.
worker.RegisterWorkflow("order-remediation", 1, async (octx, ct) =>
{
    Console.WriteLine($"      → order-remediation running (version v{octx.WorkflowVersion}).");
    var result = await octx.RunAgentAsync(OrderAnalyzeAgent(), ct);
    Console.WriteLine($"      → analyst model used: {result.ModelUsed}");

    if (state.Mode == DemoMode.Failing)
    {
        Console.WriteLine("      → remediation could not resolve the issue — marking run failed.");
        octx.MarkFailed();
        return OperationResult.Complete();
    }

    if (state.SkipApproval)
        return OperationResult.Complete();

    // Normal + Rejecting both gate a rollback on approval.
    return OperationResult.AwaitApproval(
        onApprove:      OperationResult.Next("apply_rollback", octx.Context, 30),
        onReject:       OperationResult.Complete(),
        timeoutSeconds: 120,
        justification:  "Roll back fulfillment config to last-known-good?");
});

// Operations: order-remediation v2 — uses new smart_retry_v2 tool (no rollback approval needed).
worker.RegisterWorkflow("order-remediation", 2, async (octx, ct) =>
{
    Console.WriteLine($"      → order-remediation running (version v{octx.WorkflowVersion}, smart_retry_v2).");
    var result = await octx.RunAgentAsync(OrderAnalyzeV2Agent(), ct);
    Console.WriteLine($"      → analyst model used: {result.ModelUsed}");
    return OperationResult.Complete();
});
worker.Register("order-remediation", "apply_rollback", async (octx, ct) =>
{
    await octx.RunAgentAsync(OrderRollbackAgent(), ct);
    Console.WriteLine("      → rollback_fulfillment_config applied.");
    return OperationResult.Complete();
});

// Platform: incident-diagnosis — diagnostic tools + open a ticket.
worker.RegisterWorkflow("incident-diagnosis", 1, async (octx, ct) =>
{
    await octx.RunAgentAsync(IncidentDiagnosisAgent(), ct);
    Console.WriteLine("      → open_incident_ticket created.");
    return OperationResult.Complete();
});

worker.OnApprovalRequested((req, ct) =>
{
    approvals[req.WorkflowId] = req;
    Console.WriteLine($"      ⏳ approval requested [{req.OperationName}]: {req.Justification}");
    return Task.CompletedTask;
});

var workerTask = worker.RunAsync(cts.Token);

var cp = new ControlPlaneClient(ServerAddress);
// ─────────────────────────────────────────────────────────────────────────────
Section("STEP 1 — Register workflows");

var support  = (await cp.CreateTenantAsync("support")).Id;
var ops      = (await cp.CreateTenantAsync("operations")).Id;
var platform = (await cp.CreateTenantAsync("platform")).Id;

var triage   = await Register("ticket-triage", support, 1);
var order    = await Register("order-remediation", ops, 1);
var incident = await Register("incident-diagnosis", platform, 1);

// Runtime governance policies (opaque to the server; enforced SDK-side).
await cp.SetAgentRuntimePolicyAsync(triage.Id, "triage", new AgentRuntimePolicy(MaxLlmCalls: 1));
await cp.SetAgentRuntimePolicyAsync(order.Id, "analyst", new AgentRuntimePolicy(
    MaxLlmCalls: 2,
    ToolCallLimits: [new ToolCallLimit("rollback_fulfillment_config", 1)]));

Console.WriteLine();
Console.WriteLine("  workflow             owner       version  state");
Console.WriteLine("  ───────────────────────────────────────────────");
Console.WriteLine($"  ticket-triage        support     v1       {await cp.GetWorkflowStateAsync(triage.Id)}");
Console.WriteLine($"  order-remediation    operations  v1       {await cp.GetWorkflowStateAsync(order.Id)}");
Console.WriteLine($"  incident-diagnosis   platform    v1       {await cp.GetWorkflowStateAsync(incident.Id)}");

// ─────────────────────────────────────────────────────────────────────────────
Section("STEP 2 — Trigger normal runs");

state.Mode = DemoMode.Normal;

Console.WriteLine("  ticket-triage:");
await Invoke(triage.Id);
await PromptApproval(cp, triage.Id);
await WaitActive(triage.Id);

Pause();

state.SkipApproval = true;
Console.WriteLine("  order-remediation (runs analyst, no rollback needed):");
await Invoke(order.Id);
await WaitActive(order.Id);
state.SkipApproval = false;

Pause();

Console.WriteLine("  incident-diagnosis (diagnostic only):");
await Invoke(incident.Id);
await WaitActive(incident.Id);

// ─────────────────────────────────────────────────────────────────────────────
Section("STEP 3 — Loop detection escalates model");

state.SkipApproval = true;
Console.WriteLine("  Setting agent lifecycle policy: if retry_fulfillment_job called ≥ 3 times last run → escalate to Opus.");
var orderLoopDetect = await Register("order-remediation", ops, 1);
await cp.SetAgentLifecyclePolicyAsync(orderLoopDetect.Id, "analyst", new AgentLifecyclePolicy([
    new AgentLifecycleRule(
        Metric:    AgentMetric.CallsPerTool,
        Operator:  PolicyOperator.GreaterThanOrEqual,
        Threshold: 3,
        Window:    1,
        Action:    new PolicyMutation(PolicyField.Model, OpusModel),
        ToolName:  "retry_fulfillment_job")
]));

Console.WriteLine($"  Simulating analyst stuck in a retry loop (expected model this run: {SonnetModel}).");
state.Looping = true;
await Invoke(orderLoopDetect.Id);
await WaitActive(orderLoopDetect.Id);

Pause("↑ loop recorded — next run will fire the escalation policy.");

Console.WriteLine("  Next run — lifecycle policy should have escalated the model:");
state.Looping = false;
await Invoke(orderLoopDetect.Id);
await WaitActive(orderLoopDetect.Id);
Console.WriteLine($"  (expected model this run: {OpusModel})");

// ─────────────────────────────────────────────────────────────────────────────
Section("STEP 4/5 — Cost governance + agent lifecycle policy");

state.SkipApproval = true;

Console.WriteLine("  Setting agent lifecycle policy: if cost ≥ $1 last run → switch analyst to Haiku.");
await cp.SetAgentLifecyclePolicyAsync(order.Id, "analyst", new AgentLifecyclePolicy([
    new AgentLifecycleRule(
        Metric:    AgentMetric.CostUsd,
        Operator:  PolicyOperator.GreaterThanOrEqual,
        Threshold: 1,
        Window:    1,
        Action:    new PolicyMutation(PolicyField.Model, HaikuModel))
]));

Console.WriteLine($"  Simulating order-remediation cost spike (expected model this run: {SonnetModel}).");
state.CostSpike = true;
await Invoke(order.Id);
await WaitActive(order.Id);

Pause("↑ cost spike recorded — next run will fire the downgrade policy.");

Console.WriteLine("  Next run — lifecycle policy should switch the model:");
state.CostSpike = false;
await Invoke(order.Id);
await WaitActive(order.Id);
Console.WriteLine($"  (expected model this run: {HaikuModel})");

// ─────────────────────────────────────────────────────────────────────────────
Section("STEP 6/7 — Workflow lifecycle policy reacts to a bad v2");

state.SkipApproval = false;

// 7a — repeated failures → cooldown.
Console.WriteLine("  (a) failures → COOLDOWN");
var wfCooldown = await Register("order-remediation", ops, 1);
await cp.SetWorkflowLifecyclePolicyAsync(wfCooldown.Id, new WorkflowLifecyclePolicy([
    new WorkflowLifecyclePolicyRule(WorkflowMetric.NumFailures, 1, new CooldownAction(Window: 1, CooldownSeconds: 15))
]));
state.Mode = DemoMode.Failing;
await Invoke(wfCooldown.Id);
await WaitActiveOr(wfCooldown.Id, WorkflowState.Cooldown);
Console.WriteLine($"      → order-remediation state: {await cp.GetWorkflowStateAsync(wfCooldown.Id)}");

Pause();

// 7b — approval rejections → pause.
Console.WriteLine("  (b) approval rejections → PAUSE");
var wfPause = await Register("order-remediation", ops, 1);
await cp.SetWorkflowLifecyclePolicyAsync(wfPause.Id, new WorkflowLifecyclePolicy([
    new WorkflowLifecyclePolicyRule(WorkflowMetric.ApprovalRejections, 1, new PauseAction(Window: 1))
]));
state.Mode = DemoMode.Rejecting;
await Invoke(wfPause.Id);
Console.WriteLine("      (press n to reject and trigger the pause policy)");
await PromptApproval(cp, wfPause.Id);
await WaitState(wfPause.Id, WorkflowState.Paused);
Console.WriteLine($"      → order-remediation state: {await cp.GetWorkflowStateAsync(wfPause.Id)}");

Pause();

// 7c — v2's new tool fails → ToolFailureRate → roll back v2 → v1.
Console.WriteLine("  (c) v2 ships new smart_retry_v2 tool — it's flaky → ToolFailureRate → ROLLBACK v2 → v1");
var wfVersion = await Register("order-remediation", ops, 2);
await cp.SetWorkflowLifecyclePolicyAsync(wfVersion.Id, new WorkflowLifecyclePolicy([
    new WorkflowLifecyclePolicyRule(WorkflowMetric.ToolFailureRate, 1, new SetVersionAction(TargetVersion: 1), "smart_retry_v2")
]));
state.Mode = DemoMode.Normal;
Console.WriteLine("      → invoking v2 (smart_retry_v2 will fail, triggering tool-failure rollback)...");
await Invoke(wfVersion.Id);
await WaitActive(wfVersion.Id);
state.SkipApproval = true;
Console.WriteLine("      → next invoke should run v1 (rolled back to retry_fulfillment_job):");
await Invoke(wfVersion.Id);
await WaitActive(wfVersion.Id);

Section("Demo complete");
cts.Cancel();
try { await workerTask; } catch { /* expected on cancellation */ }
return;

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

async Task<Workflow> Register(string type, string tenantId, int version)
{
    var wf = await cp.CreateWorkflowAsync(type, tenantId, new WorkflowConfig(Version: version));
    await cp.ActivateWorkflowAsync(wf.Id);
    return wf;
}

async Task Invoke(string id) =>
    await cp.InvokeWorkflowAsync(id, new RuntimeOverrides(OperationTimeoutSeconds: 60));

async Task<LifecycleState> WaitActive(string id) => await WaitLifecycle(id, LifecycleState.Active);

async Task<LifecycleState> WaitLifecycle(string id, LifecycleState target)
{
    LifecycleState s;
    do { await Task.Delay(500); s = await cp.GetWorkflowLifecycleStateAsync(id); }
    while (s != target);
    return s;
}

async Task WaitState(string id, WorkflowState target)
{
    WorkflowState? s;
    do { await Task.Delay(500); s = await cp.GetWorkflowStateAsync(id); }
    while (s != target);
}

// Waits until the workflow is no longer Invoking, then returns. Covers runs that
// end Active or transition to a non-active workflow_state (cooldown/paused).
async Task WaitActiveOr(string id, WorkflowState alt)
{
    while (true)
    {
        await Task.Delay(500);
        var life = await cp.GetWorkflowLifecycleStateAsync(id);
        if (life == LifecycleState.Invoking) continue;
        var wf = await cp.GetWorkflowStateAsync(id);
        if (wf == alt || life == LifecycleState.Active) return;
    }
}

async Task<ApprovalRequest> WaitForApproval(string id)
{
    await WaitLifecycle(id, LifecycleState.AwaitingApproval);
    while (!approvals.ContainsKey(id)) await Task.Delay(200);
    approvals.TryRemove(id, out var req);
    return req!;
}

async Task PromptApproval(ControlPlaneClient client, string id)
{
    var req = await WaitForApproval(id);
    Console.Write($"      [y/n] {req.Justification} → ");
    char key;
    do { key = char.ToLower(Console.ReadKey(intercept: true).KeyChar); }
    while (key != 'y' && key != 'n');
    if (key == 'y')
    {
        Console.WriteLine("approved ✔");
        await client.ApproveWorkflowAsync(id, req.ApprovalId);
    }
    else
    {
        Console.WriteLine("rejected ✘");
        await client.RejectWorkflowAsync(id, req.ApprovalId);
    }
}

static MockTurn Turn(int inTok, int outTok, params string[] tools) =>
    new([.. tools.Select(t => new MockToolCall(t))], inTok, outTok);

static MockTurn Submit() =>
    new([new MockToolCall("submit_result", JsonNode.Parse("{\"summary\":\"done\"}"))]);

static void Section(string title)
{
    Console.Write("  [any key to continue] ");
    Console.ReadKey(intercept: true);
    Console.Clear();
    Console.WriteLine($"══ {title} ".PadRight(70, '═'));
    Console.WriteLine();
}

static void Pause(string hint = "")
{
    if (!string.IsNullOrEmpty(hint)) Console.WriteLine($"  {hint}");
    Console.Write("  [any key to continue] ");
    Console.ReadKey(intercept: true);
    Console.WriteLine();
}

// ── Agent definitions (the system-prompt tag drives the mock) ────────────────

static AgentDefinition TicketTriageAgent() => new(
    Name:         "triage",
    SystemPrompt: "[ticket-triage] You are a support triage agent. Read-only tools only.",
    Model:        SonnetModel,
    AllowedCallbacks: [
        ReadTool("get_ticket"), ReadTool("search_kb")
    ],
    OutputSchema: JsonNode.Parse("{\"summary\":{\"type\":\"string\"}}"));

static AgentDefinition OrderAnalyzeAgent() => new(
    Name:         "analyst",
    SystemPrompt: "[order-analyze] You diagnose stuck orders. Retry is allowed; pause/rollback need approval.",
    Model:        SonnetModel,
    AllowedCallbacks: [
        ReadTool("get_order_status"), ReadTool("get_queue_depth"), ReadTool("get_worker_logs"),
        ReadTool("retry_fulfillment_job")
    ],
    OutputSchema: JsonNode.Parse("{\"summary\":{\"type\":\"string\"}}"));

static AgentDefinition OrderRollbackAgent() => new(
    Name:         "rollback",
    SystemPrompt: "[order-rollback] Apply the approved rollback.",
    Model:        SonnetModel,
    AllowedCallbacks: [ReadTool("rollback_fulfillment_config")],
    OutputSchema: JsonNode.Parse("{\"summary\":{\"type\":\"string\"}}"));

static AgentDefinition IncidentDiagnosisAgent() => new(
    Name:         "diagnoser",
    SystemPrompt: "[incident-diagnosis] You diagnose incidents. Diagnostic tools only, no mutating actions.",
    Model:        SonnetModel,
    AllowedCallbacks: [
        ReadTool("get_service_health"), ReadTool("get_recent_deployments"),
        ReadTool("get_logs"), ReadTool("open_incident_ticket")
    ],
    OutputSchema: JsonNode.Parse("{\"summary\":{\"type\":\"string\"}}"));

static AgentDefinition OrderAnalyzeV2Agent() => new(
    Name:         "analyst",
    SystemPrompt: "[order-analyze-v2] Diagnose stuck orders using the new smart retry service.",
    Model:        SonnetModel,
    AllowedCallbacks: [
        ReadTool("get_order_status"), ReadTool("get_queue_depth"),
        FlakyTool("smart_retry_v2")
    ],
    OutputSchema: JsonNode.Parse("{\"summary\":{\"type\":\"string\"}}"));

static AllowedCallback ReadTool(string name) => new(
    Name:        name,
    Description: name,
    Handler:     (input, ct) => Task.FromResult<JsonNode?>(JsonValue.Create("ok")));

static AllowedCallback FlakyTool(string name) => new(
    Name:        name,
    Description: name,
    Handler:     (_, _) => Task.FromException<JsonNode?>(new Exception($"{name}: downstream service unavailable")));

// ── Shared demo state the handlers + mock read ───────────────────────────────

internal enum DemoMode { Normal, Failing, Rejecting }

internal sealed class DemoState
{
    public volatile DemoMode Mode = DemoMode.Normal;
    public volatile bool CostSpike;
    public volatile bool Looping;
    public volatile bool SkipApproval;
}
