using Convergeplane.V1;
using Grpc.Core;
using Grpc.Net.Client;
using Microsoft.Extensions.Logging;

namespace BoundFlow.SDK.Worker;

/// <summary>
/// Called when the server dispatches an operation to this worker.
/// Return a completed AtomicOperationResult to advance or complete the job,
/// or throw to fail it. The CancellationToken is cancelled if the server sends Cancel.
/// </summary>
internal delegate Task<AtomicOperationResult> OperationHandler(
    AtomicOperation operation,
    CancellationToken ct
);

/// <summary>
/// Connects to the BoundFlow server and drives the worker session loop.
/// </summary>
internal sealed class WorkerClient : IAsyncDisposable
{
    private readonly GrpcChannel _channel;
    private readonly WorkerService.WorkerServiceClient _client;
    private readonly string _sessionId;
    private readonly ILogger<WorkerClient> _logger;

    public WorkerClient(string serverAddress, ILogger<WorkerClient> logger)
    {
        _channel = GrpcChannel.ForAddress(serverAddress);
        _client = new WorkerService.WorkerServiceClient(_channel);
        _sessionId = Guid.NewGuid().ToString();
        _logger = logger;
    }

    /// <summary>
    /// Opens a session with the server and runs the worker loop until cancellation.
    /// </summary>
    public async Task RunAsync(OperationHandler onOperation, CancellationToken ct = default)
    {
        using var stream = _client.WorkerSession(cancellationToken: ct);

        _logger.LogInformation("Session started. SessionId={SessionId}", _sessionId);

        await SendAsync(stream, new WorkerMessage { SessionId = _sessionId, Ready = new ReadyForWork() }, ct);

        // Track the in-flight operation so Cancel can reach it.
        Task? opTask = null;
        CancellationTokenSource? opCts = null;
        string? opId = null;

        while (await stream.ResponseStream.MoveNext(ct))
        {
            var command = stream.ResponseStream.Current;
            switch (command.PayloadCase)
            {
                case ServerCommand.PayloadOneofCase.Launch:
                    var op = command.Launch.Operation;
                    opId = op.Id;
                    opCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
                    if (op.TimeoutSeconds > 0)
                        opCts.CancelAfter(TimeSpan.FromSeconds(op.TimeoutSeconds));

                    // Ack IN_PROGRESS before starting — keep the receive loop free.
                    await SendAsync(stream, OperationUpdate(op.Id, OperationStatus.InProgress), ct);

                    opTask = RunOperationAsync(stream, op, onOperation, opCts.Token);
                    break;

                case ServerCommand.PayloadOneofCase.Cancel:
                    var cancelId = command.Cancel.OperationId;
                    _logger.LogWarning("Cancel received. OperationId={OperationId}", cancelId);

                    if (opCts is not null && cancelId == opId)
                    {
                        await opCts.CancelAsync();
                        if (opTask is not null)
                            await opTask; // RunOperationAsync sends CANCELLED, just wait for it
                    }

                    opTask = null;
                    opCts = null;
                    opId = null;

                    await SendAsync(stream, new WorkerMessage { SessionId = _sessionId, Ready = new ReadyForWork() }, ct);
                    break;

                default:
                    _logger.LogWarning("Unknown command. PayloadCase={PayloadCase}", command.PayloadCase);
                    break;
            }
        }

        _logger.LogInformation("Session ended. SessionId={SessionId}", _sessionId);
    }

    private async Task RunOperationAsync(
        AsyncDuplexStreamingCall<WorkerMessage, ServerCommand> stream,
        AtomicOperation op,
        OperationHandler onOperation,
        CancellationToken ct)
    {
        _logger.LogInformation("Operation started. OperationId={OperationId} Name={Name}", op.Id, op.Name);

        AtomicOperationResult result;
        try
        {
            result = await onOperation(op, ct);
        }
        catch (OperationCanceledException)
        {
            _logger.LogWarning("Operation cancelled. OperationId={OperationId}", op.Id);
            await SendAsync(stream, OperationUpdate(op.Id, OperationStatus.Cancelled), CancellationToken.None);
            return;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Operation handler threw. OperationId={OperationId}", op.Id);
            await SendAsync(stream, OperationUpdate(op.Id, OperationStatus.Failed, ex.Message), ct);
            return;
        }

        _logger.LogInformation("Operation complete. OperationId={OperationId} Status={Status}", op.Id, result.Status);
        await SendAsync(stream, new WorkerMessage
        {
            SessionId = _sessionId,
            Update = new OperationUpdate { OperationId = op.Id, Result = result }
        }, ct);

        await SendAsync(stream, new WorkerMessage { SessionId = _sessionId, Ready = new ReadyForWork() }, ct);
    }

    private Task SendAsync(
        AsyncDuplexStreamingCall<WorkerMessage, ServerCommand> stream,
        WorkerMessage msg,
        CancellationToken ct) =>
        stream.RequestStream.WriteAsync(msg, ct);

    private WorkerMessage OperationUpdate(string operationId, OperationStatus status, string message = "") =>
        new()
        {
            SessionId = _sessionId,
            Update = new OperationUpdate
            {
                OperationId = operationId,
                Result = new AtomicOperationResult { Status = status, Message = message }
            }
        };

    public async ValueTask DisposeAsync()
    {
        await _channel.ShutdownAsync();
        _channel.Dispose();
    }
}
