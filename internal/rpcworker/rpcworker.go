package rpcworker

import (
	"context"
	"errors"
	"log/slog"
	"time"

	convergeplanev1 "github.com/convergeplane/convergeplane/gen/convergeplane/v1"
	"github.com/convergeplane/convergeplane/internal/auth"
	"github.com/convergeplane/convergeplane/internal/domain"
	"github.com/convergeplane/convergeplane/internal/storage"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
)

type RequestScheduler interface {
	CompleteRequest(ctx context.Context, req string) (bool, error)
	FailRequest(ctx context.Context, req string) (bool, error)
	MarkAwaitingApproval(ctx context.Context, resourceInstanceID string) error
}

type MetricsHandler interface {
	MergeAgentMetrics(opMetrics map[string]*convergeplanev1.AgentInvocationMetrics, jobMetrics *map[string]*convergeplanev1.AgentInvocationMetrics)
	MergeWorkflowMetrics(opMetrics domain.WorkflowJobMetrics, jobMetrics *domain.WorkflowJobMetrics)
}

type RpcWorker struct {
	convergeplanev1.UnimplementedWorkerServiceServer
	scheduler         RequestScheduler
	jobs              storage.JobRepository
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

func NewRpcWorker(jobs storage.JobRepository, id string, jobTimeout int, scheduler RequestScheduler, metrics MetricsHandler, log *slog.Logger) *RpcWorker {
	return &RpcWorker{
		jobs:              jobs,
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
func (s *RpcWorker) WorkerSession(stream grpc.BidiStreamingServer[convergeplanev1.WorkerMessage, convergeplanev1.ServerCommand]) error {
	tenantGroupID, ok := auth.TenantGroupFromContext(stream.Context())
	if !ok {
		return status.Error(codes.Unauthenticated, "missing auth")
	}

	log := s.log

	leaseExpired := make(chan bool)
	recvStream := make(chan *convergeplanev1.WorkerMessage)
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
		return stream.Send(&convergeplanev1.ServerCommand{
			Payload: &convergeplanev1.ServerCommand_Cancel{
				Cancel: &convergeplanev1.CancelOperation{
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

	completeOperation := func(cancelLease chan bool, job *domain.Job, result *convergeplanev1.AtomicOperationResult) error {
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
			_, err := s.jobs.UpdateJobWithMetrics(ctx, job.ResourceInstanceID, s.id, domain.JobStatusAwaitingNext,
				result.NextOperation.Name, int(result.NextOperation.TimeoutSeconds), result.NextOperation.Context.AsMap(), job.AgentMetrics, job.WorkflowMetrics)

			return err
		} else if result.ApprovalGate != nil {
			log.Info("operation requires approval, parking job", "request_id", job.RequestID, "resource_id", job.ResourceInstanceID, "approval_id", result.ApprovalGate.ApprovalId)

			var onApprove *domain.ApprovalBranch
			if result.ApprovalGate.OnApprove != nil {
				onApprove = &domain.ApprovalBranch{
					OperationName:  result.ApprovalGate.OnApprove.Name,
					Context:        result.ApprovalGate.OnApprove.Context.AsMap(),
					TimeoutSeconds: int(result.ApprovalGate.OnApprove.TimeoutSeconds),
				}
			}
			var onReject *domain.ApprovalBranch
			if result.ApprovalGate.OnReject != nil {
				onReject = &domain.ApprovalBranch{
					OperationName:  result.ApprovalGate.OnReject.Name,
					Context:        result.ApprovalGate.OnReject.Context.AsMap(),
					TimeoutSeconds: int(result.ApprovalGate.OnReject.TimeoutSeconds),
				}
			}

			jobMetadata := domain.JobMetadata{
				ApprovalGate: &domain.ApprovalGateMetadata{
					OnApprove: onApprove,
					OnReject:  onReject,
				},
			}

			timeoutAt := time.Now().Add(time.Duration(result.ApprovalGate.TimeoutSeconds) * time.Second)
			parked, err := s.jobs.ParkForApproval(ctx, job.ResourceInstanceID, s.id, result.ApprovalGate.ApprovalId, timeoutAt, jobMetadata, job.AgentMetrics, job.WorkflowMetrics)
			if err != nil {
				log.Error("failed to park job for approval", "request_id", job.RequestID, "resource_id", job.ResourceInstanceID, "error", err)
				return err
			}
			if !parked {
				log.Warn("lost ownership while parking job for approval", "request_id", job.RequestID, "resource_id", job.ResourceInstanceID)
			}
			if err := s.scheduler.MarkAwaitingApproval(ctx, job.ResourceInstanceID); err != nil {
				log.Warn("failed to mark resource awaiting approval, scheduler will sync", "resource_id", job.ResourceInstanceID, "error", err)
			}
		} else {
			updated, err := s.jobs.UpdateJobStatusWithMetrics(ctx, job.ResourceInstanceID, s.id, domain.JobStatusCompleted, job.AgentMetrics, job.WorkflowMetrics)
			if err != nil {
				log.Error("failed to mark job completed", "request_id", job.RequestID, "resource_id", job.ResourceInstanceID, "error", err)
				return err
			}
			if updated {
				log.Info("job completed, notifying scheduler", "request_id", job.RequestID, "resource_id", job.ResourceInstanceID)
				s.scheduler.CompleteRequest(ctx, job.RequestID)
			}
			return nil
		}
		// consider returning error for ownership failure, for now the return isnt used for anything
		return nil
	}

	failOperation := func(cancelLease chan bool, job *domain.Job) error {
		ctx := context.Background()
		defer cancelLeaseIfExists(cancelLease)

		updated, err := s.jobs.UpdateJobStatus(ctx, job.ResourceInstanceID, s.id, domain.JobStatusFailed)
		if err != nil {
			log.Error("failed to mark job failed", "request_id", job.RequestID, "resource_id", job.ResourceInstanceID, "error", err)
		} else if updated {
			log.Info("job failed, notifying scheduler", "request_id", job.RequestID, "resource_id", job.ResourceInstanceID)
			s.scheduler.FailRequest(ctx, job.RequestID)
		}

		// consider returning error for ownership failure, for now the return isnt used for anything
		return err
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

					if _, ok := msg.Payload.(*convergeplanev1.WorkerMessage_Ready); !ok {
						log.Warn("unexpected message in idle state, expected ReadyForWork")
						return errors.New("protocol error") // protocol error
					}

					log.Debug("client ready, polling for available job")
					for {
						resourceInstID, err := s.jobs.GetAvailableJob(stream.Context(), tenantGroupID)
						if resourceInstID == nil || err != nil {
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

						job, err := s.jobs.AcquireJob(stream.Context(), *resourceInstID, s.id, leaseTime, tenantGroupID)
						if job == nil || err != nil {
							if err != nil {
								log.Error("error acquiring job", "resource_id", *resourceInstID, "error", err)
							} else {
								log.Debug("failed to acquire job lease (race), retrying", "resource_id", *resourceInstID)
							}
							select {
							case <-stream.Context().Done():
								return nil
							case <-time.After(jobLookupInterval):
								continue
							}
						}

						log.Info("job acquired", "request_id", job.RequestID, "resource_id", job.ResourceInstanceID, "operation", job.CurrentAtomicOperation)

						// periodically re-up the lease
						go func(resourceInstID *string) {
							ticker := time.NewTicker(leaseWake)
							defer ticker.Stop()
							for {
								select {
								case <-cancelLease:
									log.Debug("releasing job lease", "resource_id", *resourceInstID)
									s.jobs.ReleaseJob(context.Background(), *resourceInstID, s.id)
									return
								case <-ticker.C:
									renewed, err := s.jobs.RenewJobLease(stream.Context(), *resourceInstID, s.id, leaseTime)
									if !renewed || err != nil {
										log.Warn("failed to renew job lease, expiring session", "resource_id", *resourceInstID, "error", err)
										select {
										case leaseExpired <- true:
										case <-stream.Context().Done():
										}
										return
									}
									log.Debug("job lease renewed", "resource_id", *resourceInstID)
									ticker.Reset(leaseWake)
								}
							}
						}(resourceInstID)

						resolveBranch := func(branch *domain.ApprovalBranch, label string) bool {
							if branch != nil {
								job.Context = branch.Context
								job.RuntimeParams.OperationTimeoutSeconds = branch.TimeoutSeconds
								job.CurrentAtomicOperation = branch.OperationName
								return true
							}
							// nil branch = Complete() — finish the job without launching an operation.
							log.Info("approval branch is complete, finishing job", "request_id", job.RequestID, "branch", label)
							completeOperation(cancelLease, job, &convergeplanev1.AtomicOperationResult{
								Status: convergeplanev1.OperationStatus_OPERATION_STATUS_COMPLETED,
							})
							return false
						}

						var shouldLaunch bool
						switch job.Status {
						case domain.JobStatusApproved:
							shouldLaunch = resolveBranch(job.JobMetadata.ApprovalGate.OnApprove, "on_approve")
						case domain.JobStatusRejected, domain.JobStatusAwaitingApproval:
							// JobStatusAwaitingApproval here means the approval timed out.
							// Both explicit rejection and timeout converge here — record the
							// approval rejection so workflow lifecycle policies can act on it.
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

						log.Info("sending LaunchOperation to client", "request_id", job.RequestID, "operation", job.CurrentAtomicOperation)
						err = stream.Send(&convergeplanev1.ServerCommand{
							Payload: &convergeplanev1.ServerCommand_Launch{
								Launch: &convergeplanev1.LaunchOperation{
									Operation: &convergeplanev1.AtomicOperation{
										Id:              job.RequestID,
										ResourceId:      job.ResourceInstanceID,
										OperationType:   job.JobType,
										ResourceType:    job.ResourceType,
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
						return err
					}
					return nil
				case ack := <-recvStream:
					update, ok := ack.Payload.(*convergeplanev1.WorkerMessage_Update)
					if !ok {
						log.Warn("unexpected message type while waiting for ack", "request_id", currentJob.RequestID)
						return errors.New("protocol error") // protocol error
					}
					if update.Update.OperationId != currentJob.RequestID {
						log.Warn("wrong operation id in ack", "expected", currentJob.RequestID, "got", update.Update.OperationId)
						return errors.New("wrong operation id from client")
					}
					if update.Update.Result.Status != convergeplanev1.OperationStatus_OPERATION_STATUS_IN_PROGRESS {
						log.Warn("unexpected status in ack", "request_id", currentJob.RequestID, "status", update.Update.Result.Status)
						return errors.New("unexpected status from client")
					}
					log.Info("client acked IN_PROGRESS, marking job running", "request_id", currentJob.RequestID)
					updated, err := s.jobs.UpdateJobStatus(stream.Context(), currentJob.ResourceInstanceID, s.id, domain.JobStatusRunning)
					if err != nil {
						log.Error("failed to mark job running", "request_id", currentJob.RequestID, "error", err)
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
						failOperation(cancelLease, currentJob)
						return nil
					case msg := <-recvStream:
						update, ok := msg.Payload.(*convergeplanev1.WorkerMessage_Update)
						if !ok {
							log.Warn("unexpected message type while busy", "request_id", currentJob.RequestID)
							failOperation(cancelLease, currentJob)
							return errors.New("protocol error") // protocol error
						}
						if update.Update.OperationId != currentJob.RequestID {
							log.Warn("wrong operation id while busy", "expected", currentJob.RequestID, "got", update.Update.OperationId)
							failOperation(cancelLease, currentJob)
							return errors.New("wrong operation id from client")
						}
						switch update.Update.Result.Status {
						case convergeplanev1.OperationStatus_OPERATION_STATUS_COMPLETED:
							log.Info("operation completed", "request_id", currentJob.RequestID, "resource_id", currentJob.ResourceInstanceID)
							completeOperation(cancelLease, currentJob, update.Update.Result)
						case convergeplanev1.OperationStatus_OPERATION_STATUS_FAILED:
							log.Warn("operation failed by client", "request_id", currentJob.RequestID)
							failOperation(cancelLease, currentJob)
						case convergeplanev1.OperationStatus_OPERATION_STATUS_IN_PROGRESS:
							log.Debug("operation still in progress", "request_id", currentJob.RequestID)
							continue ConnectedBusyLoop
						case convergeplanev1.OperationStatus_OPERATION_STATUS_CANCELLED: // This is unexpected
							log.Warn("unexpected CANCELLED status while busy", "request_id", currentJob.RequestID)
							failOperation(cancelLease, currentJob)
						}
						state = ConnectedIdle
						break ConnectedBusyLoop
					case <-ticker.C:
						log.Warn("job timed out, sending cancel", "request_id", currentJob.RequestID, "timeout_secs", currentJob.RuntimeParams.OperationTimeoutSeconds)
						err := cancelOperation(currentJob.RequestID)
						if err != nil {
							failOperation(cancelLease, currentJob)
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
						failOperation(cancelLease, currentJob)
						return nil
					case <-ticker.C:
						log.Warn("client did not ack cancel in time, failing job", "request_id", currentJob.RequestID)
						failOperation(cancelLease, currentJob)
						return errors.New("cancel ack timed out")
					case msg := <-recvStream:
						ack, ok := msg.Payload.(*convergeplanev1.WorkerMessage_Update)
						if !ok {
							log.Warn("unexpected message type while awaiting cancel ack", "request_id", currentJob.RequestID)
							failOperation(cancelLease, currentJob)
							return errors.New("protocol error")
						} else if ack.Update.OperationId != currentJob.RequestID {
							log.Warn("wrong operation id in cancel ack", "expected", currentJob.RequestID, "got", ack.Update.OperationId)
							failOperation(cancelLease, currentJob)
							return errors.New("wrong operation id")
						}
						switch ack.Update.Result.Status {
						case convergeplanev1.OperationStatus_OPERATION_STATUS_COMPLETED:
							log.Info("operation completed despite cancel request", "request_id", currentJob.RequestID)
							completeOperation(cancelLease, currentJob, ack.Update.Result)
						case convergeplanev1.OperationStatus_OPERATION_STATUS_FAILED,
							convergeplanev1.OperationStatus_OPERATION_STATUS_CANCELLED:
							log.Info("operation cancelled/failed", "request_id", currentJob.RequestID, "status", ack.Update.Result.Status)
							failOperation(cancelLease, currentJob)
						case convergeplanev1.OperationStatus_OPERATION_STATUS_IN_PROGRESS:
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
