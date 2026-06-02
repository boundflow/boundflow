using BoundFlow.IntegrationTests.Infrastructure;
using Xunit;

namespace BoundFlow.IntegrationTests.Scenarios;

public class PeriodicWorkflowTests : IntegrationTestBase
{
    /// <summary>
    /// Verifies that a workflow with repeat_every_seconds fires automatically
    /// without an explicit invoke, and completes at least twice within the window.
    /// </summary>
    [Fact(Skip = "Not yet implemented")]
    public Task PeriodicWorkflowFiresAutomatically() => Task.CompletedTask;

    /// <summary>
    /// Verifies that a periodic workflow in Cooldown does not fire during the
    /// cooldown window, and resumes firing after the cooldown expires.
    /// </summary>
    [Fact(Skip = "Not yet implemented")]
    public Task PeriodicWorkflowDoesNotFireDuringCooldown() => Task.CompletedTask;
}
