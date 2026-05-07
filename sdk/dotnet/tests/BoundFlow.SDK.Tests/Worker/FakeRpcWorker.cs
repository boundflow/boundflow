using System.Threading.Channels;
using Convergeplane.V1;
using Grpc.Core;

namespace BoundFlow.SDK.Tests.Worker;

/// <summary>
/// In-process fake of the server-side rpcworker. Tests drive it by writing
/// commands to Send and reading messages from Received.
/// </summary>
public sealed class FakeRpcWorker : WorkerService.WorkerServiceBase
{
    private readonly Channel<ServerCommand> _toSend =
        Channel.CreateUnbounded<ServerCommand>(new UnboundedChannelOptions { SingleReader = true });

    private readonly Channel<WorkerMessage> _received =
        Channel.CreateUnbounded<WorkerMessage>(new UnboundedChannelOptions { SingleReader = true });

    /// <summary>Queue a command for the server to send to the worker client.</summary>
    public ValueTask SendAsync(ServerCommand cmd) => _toSend.Writer.WriteAsync(cmd);

    /// <summary>Read the next message the worker client sent to the server.</summary>
    public ValueTask<WorkerMessage> NextMessageAsync(CancellationToken ct = default) =>
        _received.Reader.ReadAsync(ct);

    /// <summary>Signal that the server session is done (no more commands to send).</summary>
    public void Complete() => _toSend.Writer.Complete();

    public override async Task WorkerSession(
        IAsyncStreamReader<WorkerMessage> requestStream,
        IServerStreamWriter<ServerCommand> responseStream,
        ServerCallContext context)
    {
        var ct = context.CancellationToken;

        // Pump incoming messages into the received channel.
        var readTask = Task.Run(async () =>
        {
            while (await requestStream.MoveNext(ct))
                await _received.Writer.WriteAsync(requestStream.Current, ct);
        }, ct);

        // Forward outgoing commands to the response stream.
        await foreach (var cmd in _toSend.Reader.ReadAllAsync(ct))
            await responseStream.WriteAsync(cmd, ct);

        await readTask;
    }
}
