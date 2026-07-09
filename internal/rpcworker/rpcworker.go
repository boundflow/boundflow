package rpcworker

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"time"

	boundflowv1 "github.com/boundflow/boundflow/gen/boundflow/v1"
	"github.com/boundflow/boundflow/internal/auth"
	"github.com/boundflow/boundflow/internal/convert"
	"github.com/boundflow/boundflow/internal/domain"
	"github.com/boundflow/boundflow/internal/storage"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
)

type RequestScheduler interface {
	CompleteRequest(ctx context.Context, req string, outcome domain.RunOutcome, reason string, result map[string]any) (bool, error)
	FailRequest(ctx context.Context, req string, reason string) (bool, error)
	MarkInvoking(ctx context.Context, workflowID string) error
	MarkAwaitingApproval(ctx context.Context, workflowID string) error
}

type MetricsHandler interface {
	MergeAgentMetrics(opMetrics map[string]*boundflowv1.AgentInvocationMetrics, jobMetrics *map[string]*boundflowv1.AgentInvocationMetrics)
	MergeWorkflowMetrics(opMetrics domain.WorkflowJobMetrics, jobMetrics *domain.WorkflowJobMetrics)
}

type RpcWorker struct {
	boundflowv1.UnimplementedWorkerServiceServer
	scheduler         RequestScheduler
	jobs              storage.JobRepository
	audit             storage.AuditRepository
	id                string
	defaultJobTimeout int
	log               *slog.Logger
	metrics           MetricsHandler
}

type State int

const (
	ConnectedIdle = iota
	ConnectedWaiting
	ConnectedBusy
	CancelRequested
)

func NewRpcWorker(jobs storage.JobRepository, audit storage.AuditRepository, id string, jobTimeout int, scheduler RequestScheduler, metrics MetricsHandler, log *slog.Logger) *RpcWorker {
	return &RpcWorker{
		jobs:              jobs,
		audit:             audit,
		id:                id,
		defaultJobTimeout: jobTimeout,
		scheduler:         scheduler,
		metrics:           metrics,
		log:               log.With("component", "rpcworker", "worker_id", id),
	}
}

// The RpcWorker goes through the following state machine:
//
// ConnectedIdle
//
//	-> LaunchOperation sent
//	-> ConnectedBusy
//
// ConnectedWaiting
//
//	-> In Progress received
//	-> ConnectedBusy
//
// ConnectedBusy
//
//	-> Completed/Failed/Cancelled received
//	-> ConnectedIdle
//
// ConnectedBusy
//
//	-> Timeout exceeded
//	-> CancelRequested
//
// CancelRequested
//
//	-> Cancelled/Completed received
//	-> ConnectedIdle
//
// Any state
//
//	-> Stream disconnected
//	-> Disconnected

// TOOD: Make lease expiry reset the state to ConnectedIdle instead of closing the stream
func (s *RpcWorker) WorkerSession(stream grpc.BidiStreamingServer[boundflowv1.WorkerMessage, boundflowv1.ServerCommand]) error {
	tenantGroupID, ok := auth.TenantGroupFromContext(stream.Context())
	if !ok {
		return status.Error(codes.Unauthenticated, "missing auth")
	}

	log := s.log

	// per-session ownership id so concurrent sessions on this worker fence apart
	sessionID := uuid.NewString()

	leaseExpired := make(chan bool)
	recvStream := make(chan *boundflowv1.WorkerMessage)
	controlCodeCancelled := make(chan bool)

	go func() {
		for {
			msg, err := stream.Recv()
			if err != nil {
				return
			}
			select {
			case recvStream <- msg:
			case <-controlCodeCancelled:
			}
		}
	}()

	cancelOperation := func(operationId string) error {
		log.Warn("sending cancel to client", "operation_id", operationId)
		return stream.Send(&boundflowv1.ServerCommand{
			Payload: &boundflowv1.ServerCommand_Cancel{
				Cancel: &boundflowv1.CancelOperation{
					OperationId: operationId,
				},
			},
		})
	}

	cancelLeaseIfExists := func(cancelLease chan bool) {
		select {
		case cancelLease <- true:
		case <-stream.Context().Done():
		}
	}

	completeOperation := func(cancelLease chan bool, job *domain.Job, result *boundflowv1.AtomicOperationResult) error {
		ctx := context.Background() // request completion doesnt depend on stream context
		defer cancelLeaseIfExists(cancelLease)

		s.metrics.MergeAgentMetrics(result.AgentStateUpdates, &job.AgentMetrics)
		if result.WorkflowMetrics != nil {
			s.metrics.MergeWorkflowMetrics(
				domain.WorkflowJobMetrics{Failures: int(result.WorkflowMetrics.GetFailures())},
				&job.WorkflowMetrics,
			)
		}

		if result.NextOperation != nil {
			log.Info("operation completed with next operation, advancing job", "request_id", job.RequestID, "next_operation", result.NextOperation.Name)
			_, err := s.jobs.UpdateJobWithMetrics(ctx, job.WorkflowID, sessionID, domain.JobStatusAwaitingNext,
				result.NextOperation.Name, int(result.NextOperation.TimeoutSeconds), result.NextOperation.Context.AsMap(), job.AgentMetrics, job.WorkflowMetrics)

			if err != nil {
				return err
			}
		} else if result.ApprovalGate != nil {
			log.Info("operation requires approval, parking job", "request_id", job.RequestID, "workflow_id", job.WorkflowID, "approval_id", result.ApprovalGate.ApprovalId)

			buildBranch := func(next *boundflowv1.AtomicOperation, result *structpb.Struct) domain.ApprovalBranch {
				if next != nil {
					return domain.ApprovalBranch{
						Next: &domain.NextOperation{
							OperationName:  next.Name,
							Context:        next.Context.AsMap(),
							TimeoutSeconds: int(next.TimeoutSeconds),
						},
					}
				}
				var res map[string]any
				if result != nil {
					res = result.AsMap()
				}
				return domain.ApprovalBranch{Result: res}
			}

			jobMetadata := domain.JobMetadata{
				ApprovalGate: &domain.ApprovalGateMetadata{
					OnApprove: buildBranch(result.ApprovalGate.OnApprove, result.ApprovalGate.OnApproveResult),
					OnReject:  buildBranch(result.ApprovalGate.OnReject, result.ApprovalGate.OnRejectResult),
				},
			}

			// opened_at + timeout_at are stamped server-side (DB now()) inside ParkForApproval.
			parked, err := s.jobs.ParkForApproval(ctx, job.WorkflowID, sessionID, result.ApprovalGate.ApprovalId, int(result.ApprovalGate.TimeoutSeconds), jobMetadata, job.AgentMetrics, job.WorkflowMetrics)
			if err != nil {
				log.Error("failed to park job for approval", "request_id", job.RequestID, "workflow_id", job.WorkflowID, "error", err)
				return err
			}
			if !parked {
				log.Warn("lost ownership while parking job for approval", "request_id", job.RequestID, "workflow_id", job.WorkflowID)
			}
			if err := s.scheduler.MarkAwaitingApproval(ctx, job.WorkflowID); err != nil {
				log.Warn("failed to mark workflow awaiting approval, scheduler will sync", "workflow_id", job.WorkflowID, "error", err)
			}
		} else {
			// The SDK tags soft failures (customer_marked / uncaught_exception); a clean
			// run is successful. Server-detected outcomes (timeout, interrupted) never
			// reach here.
			outcome, reason := domain.RunOutcomeSuccessful, ""
			switch result.FailureType {
			case boundflowv1.OperationFailureType_OPERATION_FAILURE_TYPE_CUSTOMER_MARKED:
				outcome, reason = domain.RunOutcomeCustomerMarked, result.FailureReason
			case boundflowv1.OperationFailureType_OPERATION_FAILURE_TYPE_UNCAUGHT_EXCEPTION:
				outcome, reason = domain.RunOutcomeUncaughtException, result.FailureReason
			}
			var publishedResult map[string]any
			if result.Result != nil {
				publishedResult = result.Result.AsMap()
			}
			updated, err := s.jobs.UpdateJobStatusWithMetrics(ctx, job.WorkflowID, sessionID, domain.JobStatusCompleted, outcome, reason, publishedResult, job.AgentMetrics, job.WorkflowMetrics)
			if err != nil {
				log.Error("failed to mark job completed", "request_id", job.RequestID, "workflow_id", job.WorkflowID, "error", err)
				return err
			}
			if updated {
				log.Info("job completed, notifying scheduler", "request_id", job.RequestID, "workflow_id", job.WorkflowID, "outcome", outcome)
				s.scheduler.CompleteRequest(ctx, job.RequestID, outcome, reason, publishedResult)
			}
		}

		// Agent lifecycle policy actions are decided SDK-side and arrive in the
		// result; record one audit row per agent whose rules changed its effective
		// policy (the SDK only sends entries when effective != base). Best-effort.
		// Written after the job state lands so we never audit a policy action whose
		// metrics didnt persist (the branches above bail before here on a write error).
		for agent, pa := range result.AgentPolicyActions {
			details, err := json.Marshal(convert.AgentPolicyActionFromProto(agent, pa))
			if err != nil {
				log.Error("failed to marshal agent policy action", "agent", agent, "workflow_id", job.WorkflowID, "error", err)
				continue
			}
			if err := s.audit.Append(ctx, domain.AuditEvent{
				TenantGroupID: tenantGroupID,
				WorkflowID:    job.WorkflowID,
				RequestID:     job.RequestID,
				EventType:     domain.AuditEventAgentPolicyAction,
				Actor:         "system",
				OccurredAt:    time.Now(),
				Details:       details,
			}); err != nil {
				log.Error("failed to append agent policy audit", "agent", agent, "workflow_id", job.WorkflowID, "error", err)
			}
		}

		// consider returning error for ownership failure, for now the return isnt used for anything
		return nil
	}

	// failOperation interrupts the workflow (a platform failure): the scheduler records
	// the request as run_outcome=interrupted with a static reason.
	failOperation := func(cancelLease chan bool, job *domain.Job, reason string) error {
		ctx := context.Background()
		defer cancelLeaseIfExists(cancelLease)

		updated, err := s.jobs.UpdateJobStatusWithReason(ctx, job.WorkflowID, sessionID, domain.JobStatusFailed, reason)
		if err != nil {
			log.Error("failed to mark job failed", "request_id", job.RequestID, "workflow_id", job.WorkflowID, "error", err)
		} else if updated {
			log.Info("job failed, notifying scheduler", "request_id", job.RequestID, "workflow_id", job.WorkflowID)
			s.scheduler.FailRequest(ctx, job.RequestID, reason)
		}

		// consider returning error for ownership failure, for now the return isnt used for anything
		return err
	}

	// softFailOperation records a customer-domain failure (e.g. an operation timeout):
	// it bumps num_failures for lifecycle policy and completes the request with the given
	// outcome, leaving the workflow active — unlike failOperation, which interrupts it.
	softFailOperation := func(cancelLease chan bool, job *domain.Job, outcome domain.RunOutcome, reason string) {
		ctx := context.Background()
		defer cancelLeaseIfExists(cancelLease)

		job.WorkflowMetrics.Failures++
		updated, err := s.jobs.UpdateJobStatusWithMetrics(ctx, job.WorkflowID, sessionID, domain.JobStatusCompleted, outcome, reason, nil, job.AgentMetrics, job.WorkflowMetrics)
		if err != nil {
			log.Error("failed to soft-fail job", "request_id", job.RequestID, "workflow_id", job.WorkflowID, "error", err)
			return
		}
		if updated {
			log.Info("operation soft-failed, completing request", "request_id", job.RequestID, "workflow_id", job.WorkflowID, "outcome", outcome)
			s.scheduler.CompleteRequest(ctx, job.RequestID, outcome, reason, nil)
		}
	}

	go func() error {

		state := ConnectedIdle
		leaseWake := 4 * time.Second
		leaseTime := leaseWake + 1*time.Second
		jobLookupInterval := 5 * time.Second
		cancelLease := make(chan bool)
		var currentJob *domain.Job
		clientResponseTime := 3 * time.Second

		defer close(cancelLease)
		defer close(controlCodeCancelled)

		for {
			switch state {

			case ConnectedIdle:
				select {
				case <-stream.Context().Done():
					log.Info("stream disconnected in idle state")
					return nil
				case msg := <-recvStream:

					ready, ok := msg.Payload.(*boundflowv1.WorkerMessage_Ready)
					if !ok {
						log.Warn("unexpected message in idle state, expected ReadyForWork")
						return errors.New("protocol error") // protocol error
					}

					var workflowTypes []string
					var workflowVersions []int32
					for _, cap := range ready.Ready.GetCapabilities() {
						workflowTypes = append(workflowTypes, cap.GetWorkflowType())
						workflowVersions = append(workflowVersions, cap.GetWorkflowVersion())
					}
					log.Debug("client ready, polling for available job", "capabilities", len(workflowTypes))
					for {
						workflowID, err := s.jobs.GetAvailableJob(stream.Context(), tenantGroupID, workflowTypes, workflowVersions)
						if workflowID == nil || err != nil {
							if err != nil {
								log.Error("error polling for available job", "error", err)
							} else {
								log.Debug("no job available, retrying", "interval", jobLookupInterval)
							}
							select {
							case <-stream.Context().Done():
								return nil
							case <-time.After(jobLookupInterval):
								continue
							}
						}

						job, err := s.jobs.AcquireJob(stream.Context(), *workflowID, sessionID, leaseTime, tenantGroupID)
						if job == nil || err != nil {
							if err != nil {
								log.Error("error acquiring job", "workflow_id", *workflowID, "error", err)
							} else {
								log.Debug("failed to acquire job lease (race), retrying", "workflow_id", *workflowID)
							}
							select {
							case <-stream.Context().Done():
								return nil
							case <-time.After(jobLookupInterval):
								continue
							}
						}

						log.Info("job acquired", "request_id", job.RequestID, "workflow_id", job.WorkflowID, "operation", job.CurrentAtomicOperation)

						// periodically re-up the lease
						go func(workflowID *string) {
							ticker := time.NewTicker(leaseWake)
							defer ticker.Stop()
							for {
								select {
								case <-cancelLease:
									log.Debug("releasing job lease", "workflow_id", *workflowID)
									s.jobs.ReleaseJob(context.Background(), *workflowID, sessionID)
									return
								case <-ticker.C:
									renewed, err := s.jobs.RenewJobLease(stream.Context(), *workflowID, sessionID, leaseTime)
									if !renewed || err != nil {
										log.Warn("failed to renew job lease, expiring session", "workflow_id", *workflowID, "error", err)
										select {
										case leaseExpired <- true:
										case <-stream.Context().Done():
										}
										return
									}
									log.Debug("job lease renewed", "workflow_id", *workflowID)
									ticker.Reset(leaseWake)
								}
							}
						}(workflowID)

						resolveBranch := func(branch domain.ApprovalBranch, label string) bool {
							if branch.Next != nil {
								job.Context = branch.Next.Context
								job.RuntimeParams.OperationTimeoutSeconds = branch.Next.TimeoutSeconds
								job.CurrentAtomicOperation = branch.Next.OperationName
								return true
							}
							// Next nil = Complete() — finish the job without launching an operation.
							log.Info("approval branch is complete, finishing job", "request_id", job.RequestID, "branch", label)
							completionResult := &boundflowv1.AtomicOperationResult{
								Status: boundflowv1.OperationStatus_OPERATION_STATUS_COMPLETED,
							}
							if branch.Result != nil {
								if s, err := structpb.NewStruct(branch.Result); err == nil {
									completionResult.Result = s
								}
							}
							completeOperation(cancelLease, job, completionResult)
							return false
						}

						var shouldLaunch bool
						switch job.Status {
						case domain.JobStatusApproved:
							shouldLaunch = resolveBranch(job.JobMetadata.ApprovalGate.OnApprove, "on_approve")
						case domain.JobStatusRejected:
							// Explicit rejection, or a timeout the scheduler already resolved
							// to rejected. Record the approval rejection so workflow lifecycle
							// policies can act on it.
							job.WorkflowMetrics.ApprovalRejections++
							shouldLaunch = resolveBranch(job.JobMetadata.ApprovalGate.OnReject, "on_reject")
						case domain.JobStatusAwaitingNext, domain.JobStatusPending:
							shouldLaunch = true
						default:
							log.Error("unexpected job status in dispatch, skipping", "request_id", job.RequestID, "status", job.Status)
							cancelLeaseIfExists(cancelLease)
							select {
							case <-stream.Context().Done():
								return nil
							case <-time.After(jobLookupInterval):
							}
							continue
						}

						if !shouldLaunch {
							continue
						}

						contextStruct, err := structpb.NewStruct(job.Context)
						if err != nil {
							log.Error("failed to serialize job context", "request_id", job.RequestID, "error", err)
							cancelLeaseIfExists(cancelLease)
							select {
							case <-stream.Context().Done():
								return nil
							case <-time.After(jobLookupInterval):
								continue
							}
						}

						dispatched, err := s.jobs.SetJobDispatched(stream.Context(), job.WorkflowID, sessionID)
						if err != nil {
							log.Error("failed to mark job dispatched", "request_id", job.RequestID, "error", err)
							return err
						}
						if !dispatched {
							log.Warn("lost job ownership before dispatching", "request_id", job.RequestID)
							cancelLeaseIfExists(cancelLease)
							select {
							case <-stream.Context().Done():
								return nil
							case <-time.After(jobLookupInterval):
							}
							continue
						}
						// Best-effort: a worker is now handling this run. Background context so a
						// stream drop doesn't cancel it; the sweep reconciles if lost.
						if err := s.scheduler.MarkInvoking(context.Background(), job.WorkflowID); err != nil {
							log.Warn("failed to mark workflow invoking, sweep will reconcile", "workflow_id", job.WorkflowID, "error", err)
						}
						log.Info("sending LaunchOperation to client", "request_id", job.RequestID, "operation", job.CurrentAtomicOperation)
						err = stream.Send(&boundflowv1.ServerCommand{
							Payload: &boundflowv1.ServerCommand_Launch{
								Launch: &boundflowv1.LaunchOperation{
									Operation: &boundflowv1.AtomicOperation{
										Id:              job.RequestID,
										WorkflowId:      job.WorkflowID,
										OperationType:   job.JobType,
										WorkflowType:    job.WorkflowType,
										Context:         contextStruct,
										Name:            job.CurrentAtomicOperation,
										TimeoutSeconds:  int32(job.RuntimeParams.OperationTimeoutSeconds),
										WorkflowVersion: int32(job.WorkflowVersion),
									},
								},
							},
						})
						if err != nil {
							log.Error("failed to send LaunchOperation", "request_id", job.RequestID, "error", err)
							// Send errored, so the client never got the op: restore the pre-dispatch
							// status so it's re-picked, instead of failing.
							if _, uerr := s.jobs.UpdateJobStatus(context.Background(), job.WorkflowID, sessionID, job.Status); uerr != nil {
								log.Error("failed to reset dispatched job, relying on sweeper", "request_id", job.RequestID, "error", uerr)
							}
							return err
						}

						currentJob = job
						break
					}
				}
				state = ConnectedWaiting

			case ConnectedWaiting:
				log.Debug("waiting for IN_PROGRESS ack from client", "request_id", currentJob.RequestID)
				select {
				case <-stream.Context().Done():
					log.Info("stream disconnected while waiting for ack", "request_id", currentJob.RequestID)
					return nil
				case <-time.After(clientResponseTime):
					log.Warn("client did not ack in time, cancelling operation", "request_id", currentJob.RequestID)
					err := cancelOperation(currentJob.RequestID)
					if err != nil {
						failOperation(cancelLease, currentJob, "")
						return err
					}
					return nil
				case ack := <-recvStream:
					update, ok := ack.Payload.(*boundflowv1.WorkerMessage_Update)
					if !ok {
						log.Warn("unexpected message type while waiting for ack", "request_id", currentJob.RequestID)
						failOperation(cancelLease, currentJob, "")
						return errors.New("protocol error") // protocol error
					}
					if update.Update.OperationId != currentJob.RequestID {
						log.Warn("wrong operation id in ack", "expected", currentJob.RequestID, "got", update.Update.OperationId)
						failOperation(cancelLease, currentJob, "")
						return errors.New("wrong operation id from client")
					}
					if update.Update.Result.Status != boundflowv1.OperationStatus_OPERATION_STATUS_IN_PROGRESS {
						log.Warn("unexpected status in ack", "request_id", currentJob.RequestID, "status", update.Update.Result.Status)
						failOperation(cancelLease, currentJob, "")
						return errors.New("unexpected status from client")
					}
					log.Info("client acked IN_PROGRESS, marking job running", "request_id", currentJob.RequestID)
					updated, err := s.jobs.UpdateJobStatus(stream.Context(), currentJob.WorkflowID, sessionID, domain.JobStatusRunning)
					if err != nil {
						log.Error("failed to mark job running", "request_id", currentJob.RequestID, "error", err)
						failOperation(cancelLease, currentJob, "")
						return err
					}
					if !updated {
						log.Warn("lost job ownership before marking running", "request_id", currentJob.RequestID)
						return nil
					}
				}
				state = ConnectedBusy

			case ConnectedBusy:
				ticker := time.NewTicker(time.Duration(currentJob.RuntimeParams.OperationTimeoutSeconds) * time.Second)

			ConnectedBusyLoop:
				for {
					select {
					case <-stream.Context().Done():
						log.Info("stream disconnected while busy, failing job", "request_id", currentJob.RequestID)
						failOperation(cancelLease, currentJob, "")
						return nil
					case msg := <-recvStream:
						update, ok := msg.Payload.(*boundflowv1.WorkerMessage_Update)
						if !ok {
							log.Warn("unexpected message type while busy", "request_id", currentJob.RequestID)
							failOperation(cancelLease, currentJob, "")
							return errors.New("protocol error") // protocol error
						}
						if update.Update.OperationId != currentJob.RequestID {
							log.Warn("wrong operation id while busy", "expected", currentJob.RequestID, "got", update.Update.OperationId)
							failOperation(cancelLease, currentJob, "")
							return errors.New("wrong operation id from client")
						}
						switch update.Update.Result.Status {
						case boundflowv1.OperationStatus_OPERATION_STATUS_COMPLETED:
							log.Info("operation completed", "request_id", currentJob.RequestID, "workflow_id", currentJob.WorkflowID)
							completeOperation(cancelLease, currentJob, update.Update.Result)
						case boundflowv1.OperationStatus_OPERATION_STATUS_FAILED:
							log.Warn("operation failed by client", "request_id", currentJob.RequestID, "reason", update.Update.Result.Message)
							failOperation(cancelLease, currentJob, update.Update.Result.Message)
						case boundflowv1.OperationStatus_OPERATION_STATUS_IN_PROGRESS:
							log.Debug("operation still in progress", "request_id", currentJob.RequestID)
							continue ConnectedBusyLoop
						case boundflowv1.OperationStatus_OPERATION_STATUS_CANCELLED: // This is unexpected
							log.Warn("unexpected CANCELLED status while busy", "request_id", currentJob.RequestID)
							failOperation(cancelLease, currentJob, "")
						}
						state = ConnectedIdle
						break ConnectedBusyLoop
					case <-ticker.C:
						log.Warn("job timed out, sending cancel", "request_id", currentJob.RequestID, "timeout_secs", currentJob.RuntimeParams.OperationTimeoutSeconds)
						err := cancelOperation(currentJob.RequestID)
						if err != nil {
							failOperation(cancelLease, currentJob, "")
							return err
						}
						state = CancelRequested
						break ConnectedBusyLoop
					}
				}
				ticker.Stop()

			case CancelRequested:
				log.Debug("waiting for cancel ack from client", "request_id", currentJob.RequestID)
				ticker := time.NewTicker(clientResponseTime)

			CancelLoop:
				for {
					select {
					case <-stream.Context().Done():
						log.Info("stream disconnected while awaiting cancel ack", "request_id", currentJob.RequestID)
						failOperation(cancelLease, currentJob, "")
						return nil
					case <-ticker.C:
						log.Warn("client did not ack cancel in time, failing job", "request_id", currentJob.RequestID)
						failOperation(cancelLease, currentJob, "")
						return errors.New("cancel ack timed out")
					case msg := <-recvStream:
						ack, ok := msg.Payload.(*boundflowv1.WorkerMessage_Update)
						if !ok {
							log.Warn("unexpected message type while awaiting cancel ack", "request_id", currentJob.RequestID)
							failOperation(cancelLease, currentJob, "")
							return errors.New("protocol error")
						} else if ack.Update.OperationId != currentJob.RequestID {
							log.Warn("wrong operation id in cancel ack", "expected", currentJob.RequestID, "got", ack.Update.OperationId)
							failOperation(cancelLease, currentJob, "")
							return errors.New("wrong operation id")
						}
						switch ack.Update.Result.Status {
						case boundflowv1.OperationStatus_OPERATION_STATUS_COMPLETED:
							log.Info("operation completed despite cancel request", "request_id", currentJob.RequestID)
							completeOperation(cancelLease, currentJob, ack.Update.Result)
						case boundflowv1.OperationStatus_OPERATION_STATUS_FAILED,
							boundflowv1.OperationStatus_OPERATION_STATUS_CANCELLED:
							// The client acked our cancel, so the operation timed out cleanly:
							// a customer-domain failure (num_failures), not a platform interruption.
							log.Info("operation timed out (cancel acked), soft-failing", "request_id", currentJob.RequestID, "status", ack.Update.Result.Status)
							softFailOperation(cancelLease, currentJob, domain.RunOutcomeOperationTimeout, "operation exceeded its timeout")
						case boundflowv1.OperationStatus_OPERATION_STATUS_IN_PROGRESS:
							log.Debug("still in progress during cancel, waiting", "request_id", currentJob.RequestID)
							continue
						}
						break CancelLoop
					}
				}
				ticker.Stop()
				state = ConnectedIdle
			}
		}
	}()

	select {
	case <-leaseExpired:
		log.Warn("session ending due to lease expiry")
	case <-stream.Context().Done():
		log.Info("session ending due to stream close")
	case <-controlCodeCancelled:
		log.Info("session ending, worker goroutine exited")
	}

	return nil
}
