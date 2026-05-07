using System.Text.Json.Nodes;

namespace BoundFlow.ControlPlane;

public record TenantGroup(
    string Id,
    string Name
);

public record Tenant(
    string Id,
    string Name,
    string TenantGroupId
);

public record ResourceInstance(
    string Id,
    string TenantId,
    JsonNode? GoalState
);

public enum LifecycleState
{
    Unknown,
    Creating,
    Active,
    Reconciling,
    Deleting,
    Deleted,
    Failed,
}

public record ResourceState(
    JsonNode? CurrentConfigState,
    JsonNode? GoalConfigState,
    LifecycleState LifecycleState
);
