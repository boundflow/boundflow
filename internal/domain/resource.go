package domain

import "time"

type LifecycleState string

const (
	LifecycleStateCreating    LifecycleState = "creating"
	LifecycleStateActive      LifecycleState = "active"
	LifecycleStateReconciling LifecycleState = "reconciling"
	LifecycleStateDeleting    LifecycleState = "deleting"
	LifecycleStateDeleted     LifecycleState = "deleted"
	LifecycleStateFailed      LifecycleState = "failed"
)

type ResourceInstance struct {
	ID                     string
	TenantID               string
	ResourceType           string
	CurrentConfigState     ResourceState
	ConfigGoalState        ResourceState
	LifecycleState         LifecycleState
	SchedulerPartitionID   string
	Version                int64
	LastCompletedRequestAt *time.Time
	CreatedAt              time.Time
}
