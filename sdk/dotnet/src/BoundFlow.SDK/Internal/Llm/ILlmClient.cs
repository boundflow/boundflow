using Anthropic.SDK;
using Anthropic.SDK.Messaging;

namespace BoundFlow.SDK.Llm;

/// <summary>
/// The single LLM call the orchestrator depends on. Implemented by the real Anthropic
/// client in production and by a scripted mock for deterministic demos and tests.
/// </summary>
public interface ILlmClient
{
    Task<MessageResponse> GetClaudeMessageAsync(MessageParameters request, CancellationToken ct = default);
}

/// <summary>Production implementation backed by the Anthropic API.</summary>
internal sealed class AnthropicLlmClient : ILlmClient
{
    private readonly AnthropicClient _client;

    public AnthropicLlmClient(AnthropicClient client) => _client = client;

    public Task<MessageResponse> GetClaudeMessageAsync(MessageParameters request, CancellationToken ct = default)
        => _client.Messages.GetClaudeMessageAsync(request, ct);
}
