using BoundFlow.SDK.Worker;
using Convergeplane.V1;
using Xunit;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;

namespace BoundFlow.SDK.Tests.Worker;

public sealed class WorkerClientTests : IAsyncLifetime
{
    private FakeRpcWorker _fake = null!;
    private WebApplication _app = null!;
    private string _address = null!;

    public async Task InitializeAsync()
    {
        _fake = new FakeRpcWorker();

        // Pick a random free port.
        var port = Random.Shared.Next(50100, 51000);
        _address = $"http://localhost:{port}";

        var builder = WebApplication.CreateBuilder();
        builder.Services.AddGrpc();
        builder.Services.AddSingleton(_fake);
        builder.WebHost.ConfigureKestrel(o =>
            o.ListenLocalhost(port, l => l.Protocols = HttpProtocols.Http2));

        _app = builder.Build();
        _app.MapGrpcService<FakeRpcWorker>();
        await _app.StartAsync();
    }

    public async Task DisposeAsync()
    {
        _fake.Complete();
        await _app.StopAsync();
        await _app.DisposeAsync();
    }

    // -------------------------------------------------------------------------

    [Fact]
    public async Task HappyPath_OperationCompletedWithNoNextOperation()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var op = MakeOperation("op-1");

        // Server will launch the operation then close the session.
        _ = Task.Run(async () =>
        {
            // Wait for ReadyForWork.
            var ready = await _fake.NextMessageAsync(cts.Token);
            Assert.Equal(WorkerMessage.PayloadOneofCase.Ready, ready.PayloadCase);

            // Send LaunchOperation.
            await _fake.SendAsync(new ServerCommand { Launch = new LaunchOperation { Operation = op } });

            // Expect IN_PROGRESS ack.
            var inProgress = await _fake.NextMessageAsync(cts.Token);
            Assert.Equal(OperationStatus.InProgress, inProgress.Update.Result.Status);

            // Expect COMPLETED.
            var completed = await _fake.NextMessageAsync(cts.Token);
            Assert.Equal(OperationStatus.Completed, completed.Update.Result.Status);

            // Expect ReadyForWork again.
            var ready2 = await _fake.NextMessageAsync(cts.Token);
            Assert.Equal(WorkerMessage.PayloadOneofCase.Ready, ready2.PayloadCase);

            _fake.Complete();
        }, cts.Token);

        var client = new WorkerClient(_address, NullLogger<WorkerClient>.Instance);
        await client.RunAsync((operation, ct) =>
        {
            Assert.Equal("op-1", operation.Id);
            return Task.FromResult(new AtomicOperationResult { Status = OperationStatus.Completed });
        }, cts.Token);
    }

    [Fact]
    public async Task Cancel_OperationTokenIsCancelledAndCancelledStatusSent()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var op = MakeOperation("op-2", timeoutSeconds: 60);
        var operationStarted = new TaskCompletionSource();
        var operationCancelled = new TaskCompletionSource();

        _ = Task.Run(async () =>
        {
            var ready = await _fake.NextMessageAsync(cts.Token);
            Assert.Equal(WorkerMessage.PayloadOneofCase.Ready, ready.PayloadCase);

            await _fake.SendAsync(new ServerCommand { Launch = new LaunchOperation { Operation = op } });

            // Wait for IN_PROGRESS.
            var inProgress = await _fake.NextMessageAsync(cts.Token);
            Assert.Equal(OperationStatus.InProgress, inProgress.Update.Result.Status);

            // Signal the test that the operation has started.
            operationStarted.SetResult();

            // Send Cancel.
            await _fake.SendAsync(new ServerCommand { Cancel = new CancelOperation { OperationId = "op-2" } });

            // Expect CANCELLED status back.
            var cancelled = await _fake.NextMessageAsync(cts.Token);
            Assert.Equal(OperationStatus.Cancelled, cancelled.Update.Result.Status);

            operationCancelled.SetResult();
            _fake.Complete();
        }, cts.Token);

        var client = new WorkerClient(_address, NullLogger<WorkerClient>.Instance);
        await client.RunAsync(async (operation, ct) =>
        {
            operationStarted.Task.Wait(cts.Token);
            // Block until cancelled.
            await Task.Delay(Timeout.Infinite, ct);
            return new AtomicOperationResult { Status = OperationStatus.Completed };
        }, cts.Token);

        await operationCancelled.Task.WaitAsync(cts.Token);
    }

    [Fact]
    public async Task Timeout_OperationCancelledLocallyWhenTimeoutExpires()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        // 1 second timeout on the operation.
        var op = MakeOperation("op-3", timeoutSeconds: 1);
        var tokenWasCancelled = false;

        _ = Task.Run(async () =>
        {
            var ready = await _fake.NextMessageAsync(cts.Token);
            Assert.Equal(WorkerMessage.PayloadOneofCase.Ready, ready.PayloadCase);

            await _fake.SendAsync(new ServerCommand { Launch = new LaunchOperation { Operation = op } });

            // Expect IN_PROGRESS.
            var inProgress = await _fake.NextMessageAsync(cts.Token);
            Assert.Equal(OperationStatus.InProgress, inProgress.Update.Result.Status);

            // Expect FAILED (timeout fires, OperationCanceledException → Failed).
            var failed = await _fake.NextMessageAsync(cts.Token);
            Assert.Equal(OperationStatus.Failed, failed.Update.Result.Status);

            _fake.Complete();
        }, cts.Token);

        var client = new WorkerClient(_address, NullLogger<WorkerClient>.Instance);
        await client.RunAsync(async (operation, ct) =>
        {
            try
            {
                await Task.Delay(Timeout.Infinite, ct);
            }
            catch (OperationCanceledException)
            {
                tokenWasCancelled = true;
                throw;
            }
            return new AtomicOperationResult { Status = OperationStatus.Completed };
        }, cts.Token);

        Assert.True(tokenWasCancelled);
    }

    // -------------------------------------------------------------------------

    private static AtomicOperation MakeOperation(string id, int timeoutSeconds = 30) =>
        new() { Id = id, Name = id, ResourceId = "res-1", TimeoutSeconds = timeoutSeconds };
}
