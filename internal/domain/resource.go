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

type WorkflowConfig struct {
	InitialVersion       int32
	InvokeTimeoutSeconds int32
	RepeatEverySeconds   int32
	Triggerable          bool
}

type ResourceInstance struct {
	ID                     string
	TenantID               string
	ResourceType           string
	WorkflowConfig         WorkflowConfig
	LifecycleState         LifecycleState
	SchedulerPartitionID   string
	TargetVersion          int64
	CurrentVersion         int64
	LastCompletedRequestAt *time.Time
	CreatedAt              time.Time
}
