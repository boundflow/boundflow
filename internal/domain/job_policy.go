package domain

// WorkflowRuntimeParams holds the resolved runtime parameters for a workflow invocation.
// Fields are resolved at request creation time from (in order): the per-invocation
// RuntimeOverrides, the WorkflowConfig, the tenant's policy overrides, and the tenant group's policies.
type WorkflowRuntimeParams struct {
	InitialWorkflowVersion  int
	OperationTimeoutSeconds int
}
