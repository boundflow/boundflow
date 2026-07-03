package domain

import "time"

type CustomerRequestStatus string

const (
	CustomerRequestStatusUnscheduled CustomerRequestStatus = "unscheduled"
	CustomerRequestStatusScheduled   CustomerRequestStatus = "scheduled"
	CustomerRequestStatusInProgress  CustomerRequestStatus = "in_progress"
	CustomerRequestStatusFailed      CustomerRequestStatus = "failed"
	CustomerRequestStatusCompleted   CustomerRequestStatus = "completed"
	CustomerRequestStatusSuperceded  CustomerRequestStatus = "superceded"
)

type CustomerRequestType string

const (
	CustomerRequestTypeCreate      CustomerRequestType = "create"
	CustomerRequestTypeInvoke      CustomerRequestType = "invoke"
	CustomerRequestTypeDelete      CustomerRequestType = "delete"
	CusomterRequestTypeHealthCheck CustomerRequestType = "healthcheck"
)

// RunOutcome is the customer-facing result of a run (one per request), set once the
// run is terminal (the in-flight state is described by request status). Internally
// only Interrupted is a failed request; the rest are completed requests, but to the
// customer every non-successful outcome is a failure with a reason.
type RunOutcome string

const (
	RunOutcomeSuccessful        RunOutcome = "successful"                   // clean success
	RunOutcomeCustomerMarked    RunOutcome = "customer_marked_failure"      // ctx.mark_failed()
	RunOutcomeUncaughtException RunOutcome = "uncaught_operation_exception" // handler raised
	RunOutcomeOperationTimeout  RunOutcome = "operation_timeout"            // op exceeded its timeout
	RunOutcomeInterrupted       RunOutcome = "interrupted"                  // platform failure
)

type CustomerRequest struct {
	ID            string
	WorkflowID    string
	Status        CustomerRequestStatus
	RequestType   CustomerRequestType
	RequestInfo   map[string]any
	Version       int64
	RunOutcome    RunOutcome
	FailureReason string
	CreatedAt     time.Time
	CompletedAt   *time.Time
}
