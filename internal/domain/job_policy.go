package domain

// JobPolicy holds the resolved policy that governs how a job's operation runs.
// Fields are resolved at request creation time from (in order): the per-request
// override, the tenant's policy overrides, and the tenant group's policies.
type JobPolicy struct {
	OperationTimeoutSeconds int
}
