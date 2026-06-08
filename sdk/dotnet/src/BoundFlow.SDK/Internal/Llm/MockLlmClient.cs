using System.Text.Json.Nodes;
using Anthropic.SDK.Common;
using Anthropic.SDK.Messaging;

namespace BoundFlow.SDK.Llm;

/// <summary>One tool the mock model "calls" on a turn. Use the name "submit_result" to finish.</summary>
public sealed record MockToolCall(string ToolName, JsonNode? Input = null);

/// <summary>
/// One scripted model turn: the tools it calls plus the token usage to report
/// (which drives cost). To finish the step, include a call to "submit_result".
/// </summary>
public sealed record MockTurn(
    IReadOnlyList<MockToolCall> ToolCalls,
    int InputTokens = 0,
    int OutputTokens = 0);

/// <summary>
/// Context handed to the mock's turn delegate. TurnIndex is 0 on the first model call of a
/// step and increments each subsequent call, so the delegate stays stateless across runs.
/// SystemPrompt lets the delegate branch on which agent is running.
/// </summary>
public sealed record MockContext(int TurnIndex, string SystemPrompt);

/// <summary>
/// A deterministic, scripted stand-in for the Anthropic client. The supplied delegate
/// decides what the model "does" on each turn given a MockContext.
/// </summary>
public sealed class MockLlmClient : ILlmClient
{
    private readonly Func<MockContext, MockTurn> _next;

    public MockLlmClient(Func<MockContext, MockTurn> next) => _next = next;

    public Task<MessageResponse> GetClaudeMessageAsync(MessageParameters request, CancellationToken ct = default)
    {
        // Honor a forced tool choice: when a policy limit is hit the orchestrator forces
        // submit_result via ToolChoice. A real model obeys, so the mock must too — otherwise
        // MaxLlmCalls wouldn't actually cap the loop.
        if (request.ToolChoice?.Type == ToolChoiceType.Tool && !string.IsNullOrEmpty(request.ToolChoice.Name))
            return Task.FromResult(Build([new MockToolCall(request.ToolChoice.Name)], 0, 0));

        // Turn index = how many assistant turns already happened this step (history grows
        // by one assistant message per prior model call).
        var turnIndex = request.Messages?.Count(m => m.Role == RoleType.Assistant) ?? 0;
        var turn = _next(new MockContext(turnIndex, request.SystemMessage ?? ""));
        return Task.FromResult(Build(turn.ToolCalls, turn.InputTokens, turn.OutputTokens));
    }

    private static MessageResponse Build(IReadOnlyList<MockToolCall> toolCalls, int inputTokens, int outputTokens)
    {
        var blocks = toolCalls
            .Select(tc => (ContentBase)new ToolUseContent
            {
                Id    = Guid.NewGuid().ToString(),
                Name  = tc.ToolName,
                Input = tc.Input ?? new JsonObject(),
            })
            .ToList();

        // MessageResponse.Message is read-only — it's derived from Content, which we set below.
        return new MessageResponse
        {
            Content    = blocks,
            StopReason = "tool_use",
            Usage      = new Usage { InputTokens = inputTokens, OutputTokens = outputTokens },
        };
    }
}
