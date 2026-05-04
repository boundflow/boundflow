package domain

import "time"

type OperationStatus int

const (
	OperationStatusUnspecified OperationStatus = iota
	OperationStatusContinue
	OperationStatusCompleted
	OperationStatusFailed
	OperationStatusRollback
)

type StepMode int

const (
	StepModeDeterministic StepMode = iota
	StepModeAgent
)

type AtomicOperation struct {
	ID            string
	ResourceID    string
	OperationType string
	Context       OperationContext
	NextOperation *AtomicOperation
	CreatedAt     time.Time
	Mode          StepMode
}

type OperationContext struct {
	Metadata map[string]string
	Payload  []byte
}

type AtomicOperationResult struct {
	Status        OperationStatus
	NextOperation *AtomicOperation
	Message       string
}
