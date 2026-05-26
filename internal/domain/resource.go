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
	InitialWorkflowVersion int32
	InvokeTimeoutSeconds   int32
	RepeatEverySeconds     int32
	Triggerable            bool
}

type WorkflowState string

const (
	WorkflowStateCreated  WorkflowState = "created"
	WorkflowStateActive   WorkflowState = "active"
	WorkflowStatePaused   WorkflowState = "paused"
	WorkflowStateCooldown WorkflowState = "cooldown"
	WorkflowStateDisabled WorkflowState = "disabled"
	WorkflowStateDeleted  WorkflowState = "deleted"
)

type ResourceInstance struct {
	ID                     string
	TenantID               string
	ResourceType           string
	WorkflowConfig         WorkflowConfig
	LifecycleState         LifecycleState
	WorkflowState          WorkflowState
	LifecyclePolicy        WorkflowLifecyclePolicy
	InvocationMetrics      []WorkflowInvocationSnapshot
	CooldownUntil          *time.Time
	CurrentWorkflowVersion int
	SchedulerPartitionID   string
	TargetVersion          int64
	CurrentVersion         int64
	LastCompletedRequestAt *time.Time
	CreatedAt              time.Time
}
