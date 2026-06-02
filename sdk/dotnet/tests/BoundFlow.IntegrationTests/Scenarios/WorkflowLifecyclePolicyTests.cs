using BoundFlow.IntegrationTests.Infrastructure;
using Xunit;

namespace BoundFlow.IntegrationTests.Scenarios;

public class WorkflowLifecyclePolicyTests : IntegrationTestBase
{
    /// <summary>
    /// Verifies that a workflow-level lifecycle rule transitions the workflow to Cooldown
    /// after a threshold of failures is reached within the rolling window.
    /// </summary>
    [Fact(Skip = "Not yet implemented")]
    public Task WorkflowEntersCooldownAfterFailureThresholdExceeded() => Task.CompletedTask;

    /// <summary>
    /// Verifies that a set_version rule triggers a version rollback when the version-total
    /// failure count exceeds the configured threshold.
    /// </summary>
    [Fact(Skip = "Not yet implemented")]
    public Task WorkflowRollsBackVersionOnVersionTotalThreshold() => Task.CompletedTask;

    /// <summary>
    /// Verifies that a pause rule fires and the workflow enters Paused state,
    /// and that it does not get scheduled again until explicitly activated.
    /// </summary>
    [Fact(Skip = "Not yet implemented")]
    public Task WorkflowPausesAndDoesNotScheduleUntilActivated() => Task.CompletedTask;
}
