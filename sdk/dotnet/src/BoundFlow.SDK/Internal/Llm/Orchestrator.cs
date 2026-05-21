using System.Text;
using System.Text.Json.Nodes;
using Anthropic.SDK;
using Anthropic.SDK.Common;
using Anthropic.SDK.Messaging;
using Microsoft.Extensions.Logging;
using AnthropicTool = Anthropic.SDK.Common.Tool;

namespace BoundFlow.SDK.Llm;

/// <summary>
/// Runs the agentic step loop against the Claude API.
/// </summary>
internal sealed class Orchestrator
{
    internal const string SubmitResultTool = "submit_result";

    private const decimal InputCostPer1M = 3.0m;
    private const decimal OutputCostPer1M = 15.0m;

    private readonly AnthropicClient _client;
    private readonly string _model;
    private readonly ILogger<Orchestrator> _logger;

    public Orchestrator(AnthropicClient client, string model, ILogger<Orchestrator> logger)
    {
        _client = client;
        _model = model;
        _logger = logger;
    }

    /// <summary>
    /// Executes an agent step to completion. The LLM may invoke allowed callbacks freely
    /// and calls submit_result when done. If a policy limit is hit before submit_result
    /// is called, one final forced call is made.
    /// </summary>
    public async Task<StepResult> RunAsync(AgentStepConfig cfg, CancellationToken ct = default)
    {
        var callbackMap = (cfg.AllowedCallbacks ?? []).ToDictionary(cb => cb.Name);
        var tools = BuildTools(cfg.AllowedCallbacks, cfg.OutputSchema);
        var messages = new List<Message>
        {
            new(RoleType.User, BuildUserContent(cfg.Objective, cfg.LlmContext))
        };

        var llmCallsUsed = 0;
        var costUsd = 0m;
        var tokensUsed = 0;
        var maxLlmCalls = cfg.Policy.MaxLlmCalls;

        while (true)
        {
            var policyLimitReached = maxLlmCalls > 0 && llmCallsUsed >= maxLlmCalls;

            if (policyLimitReached)
                _logger.LogWarning("Policy limit reached, forcing submit_result. LlmCalls={LlmCalls}", llmCallsUsed);

            _logger.LogDebug("Calling LLM. LlmCallsSoFar={LlmCalls} ForcedSubmit={Forced}", llmCallsUsed, policyLimitReached);

            var request = new MessageParameters
            {
                Model = _model,
                MaxTokens = 4096,
                SystemMessage = cfg.SystemPrompt + "\n\nWhen you have completed your objective, call the submit_result tool with your findings.",
                Messages = messages,
                Tools = tools,
                ToolChoice = policyLimitReached
                    ? new ToolChoice { Type = ToolChoiceType.Tool, Name = SubmitResultTool }
                    : new ToolChoice { Type = ToolChoiceType.Auto },
            };

            var resp = await _client.Messages.GetClaudeMessageAsync(request, ct);

            llmCallsUsed++;
            costUsd += EstimateCost(resp.Usage);
            tokensUsed += resp.Usage.InputTokens + resp.Usage.OutputTokens;

            if (cfg.Policy.MaxCostUsd > 0 && costUsd > cfg.Policy.MaxCostUsd && !policyLimitReached)
                _logger.LogWarning("Cost limit reached, will force submit_result on next call. CostUsd={Cost} Limit={Limit}", costUsd, cfg.Policy.MaxCostUsd);

            _logger.LogDebug("LLM response. StopReason={StopReason} ContentBlocks={Blocks}", resp.StopReason, resp.Content.Count);

            messages.Add(resp.Message);

            if (resp.StopReason == "end_turn")
            {
                _logger.LogWarning("Model reached end_turn without calling submit_result, forcing.");
                messages.Add(new Message(RoleType.User, "Please call submit_result with your findings."));
                maxLlmCalls = llmCallsUsed + 1;
                continue;
            }

            if (resp.StopReason != "tool_use")
                throw new InvalidOperationException($"Unexpected stop reason: {resp.StopReason}");

            var toolResults = new List<ContentBase>();
            foreach (var toolUse in resp.Content.OfType<ToolUseContent>())
            {
                _logger.LogInformation("Dispatching callback. Callback={Name}", toolUse.Name);

                var input = toolUse.Input ?? new JsonObject();

                // submit_result — capture output and return.
                if (toolUse.Name == SubmitResultTool)
                {
                    _logger.LogInformation("Agent step complete via submit_result. LlmCalls={Calls} CostUsd={Cost}", llmCallsUsed, costUsd);
                    return new StepResult(input, llmCallsUsed, costUsd, tokensUsed);
                }

                if (!callbackMap.TryGetValue(toolUse.Name, out var cb))
                {
                    _logger.LogWarning("LLM called unknown callback, reporting error. Callback={Name}", toolUse.Name);
                    toolResults.Add(new ToolResultContent { ToolUseId = toolUse.Id, Content = $"Unknown callback: {toolUse.Name}", IsError = true });
                    continue;
                }

                JsonNode? cbOutput;
                try
                {
                    cbOutput = await cb.Handler(input, ct);
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "Callback returned error, reporting to LLM. Callback={Name}", toolUse.Name);
                    toolResults.Add(new ToolResultContent { ToolUseId = toolUse.Id, Content = ex.Message, IsError = true });
                    continue;
                }

                _logger.LogInformation("Callback completed. Callback={Name}", toolUse.Name);
                toolResults.Add(new ToolResultContent
                {
                    ToolUseId = toolUse.Id,
                    Content = cbOutput?.ToJsonString() ?? "{}",
                    IsError = false
                });
            }

            if (toolResults.Count > 0)
                messages.Add(new Message { Role = RoleType.User, Content = toolResults });

            if (cfg.Policy.MaxCostUsd > 0 && costUsd > cfg.Policy.MaxCostUsd)
                maxLlmCalls = llmCallsUsed;
        }
    }

    private static List<AnthropicTool> BuildTools(
        IReadOnlyList<AllowedCallback>? callbacks,
        JsonNode? outputSchema)
    {
        var tools = new List<AnthropicTool>();

        foreach (var cb in callbacks ?? [])
        {
            var desc = string.IsNullOrEmpty(cb.Description) ? cb.Name : cb.Description;
            if (!string.IsNullOrEmpty(cb.Mode))
                desc = $"{desc} [{cb.Mode}]";

            tools.Add(new Function(cb.Name, desc, WrapSchema(cb.InputSchema)));
        }

        tools.Add(new Function(
            SubmitResultTool,
            "Call this when you have completed your objective to submit your final result.",
            WrapSchema(outputSchema)));

        return tools;
    }

    private static string BuildUserContent(string objective, IReadOnlyList<LlmContextEntry>? llmContext)
    {
        var sb = new StringBuilder();
        sb.AppendLine($"Objective: {objective}");

        if (llmContext is { Count: > 0 })
        {
            sb.AppendLine("\nContext:");
            foreach (var entry in llmContext)
                sb.AppendLine($"- {entry.Metadata}: {entry.Payload?.ToJsonString() ?? "null"}");
        }

        return sb.ToString();
    }

    // Wraps a properties map into a full JSON Schema object as required by the Anthropic API.
    private static JsonNode WrapSchema(JsonNode? properties) =>
        JsonNode.Parse($"{{\"type\":\"object\",\"properties\":{properties?.ToJsonString() ?? "{}"}}}") !;

    private static decimal EstimateCost(Usage usage) =>
        usage.InputTokens / 1_000_000m * InputCostPer1M +
        usage.OutputTokens / 1_000_000m * OutputCostPer1M;
}
