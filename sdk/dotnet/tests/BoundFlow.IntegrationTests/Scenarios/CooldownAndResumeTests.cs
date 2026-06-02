using BoundFlow.IntegrationTests.Infrastructure;
using Xunit;

namespace BoundFlow.IntegrationTests.Scenarios;

public class CooldownAndResumeTests : IntegrationTestBase
{
    /// <summary>
    /// Verifies that a workflow manually placed in Cooldown cannot be invoked,
    /// and that it automatically transitions back to Active once the cooldown expires.
    /// </summary>
    [Fact(Skip = "Not yet implemented")]
    public Task WorkflowResumesAfterCooldownExpires() => Task.CompletedTask;

    /// <summary>
    /// Verifies that an invoke request submitted while a workflow is in Cooldown
    /// is queued and executed once the workflow becomes Active again.
    /// </summary>
    [Fact(Skip = "Not yet implemented")]
    public Task InvokeWhileInCooldownExecutesAfterResume() => Task.CompletedTask;
}
