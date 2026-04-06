package rpcworker

import (
	"context"
	"errors"
	"time"

	convergeplanev1 "github.com/convergeplane/convergeplane/gen/convergeplane/v1"
	"github.com/convergeplane/convergeplane/internal/domain"
	"github.com/convergeplane/convergeplane/internal/storage"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/structpb"
)

type RequestScheduler interface {
	CompleteRequest(ctx context.Context, req string) (bool, error)
	FailRequest(ctx context.Context, req string) (bool, error)
}

type RpcWorker struct {
	convergeplanev1.UnimplementedWorkerServiceServer
	scheduler         RequestScheduler
	jobs              storage.JobRepository
	id                string
	defaultJobTimeout int
}

type State int

const (
	ConnectedIdle = iota
	ConnectedWaiting
	ConnectedBusy
	CancelRequested
)

func NewRpcWorker(jobs storage.JobRepository, id string, jobTimeout int, scheduler RequestScheduler) *RpcWorker {
	return &RpcWorker{
		jobs:              jobs,
		id:                id,
		defaultJobTimeout: jobTimeout,
		scheduler:         scheduler,
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
func (s *RpcWorker) WorkerSession(stream grpc.BidiStreamingServer[convergeplanev1.WorkerMessage, convergeplanev1.ServerCommand]) error {

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

		if result.NextOperation == nil {
			updated, err := s.jobs.UpdateJobStatus(ctx, job.ResourceInstanceID, s.id, domain.JobStatusCompleted)
			if err != nil {
				return err
			}
			if updated {
				s.scheduler.CompleteRequest(ctx, job.RequestID)
			}
			return nil
		}
		_, err := s.jobs.UpdateJob(ctx, job.ResourceInstanceID, s.id, domain.JobStatusAwaitingNext,
			result.NextOperation.Name, result.NextOperation.Context.AsMap())

		// consider returning error for ownership failure, for now the return isnt used for anything
		return err
	}

	failOperation := func(cancelLease chan bool, job *domain.Job) error {
		// in the future maybe we have retry policies on the operation or something, but for now a fail is a request fail
		ctx := context.Background()
		defer cancelLeaseIfExists(cancelLease)

		updated, err := s.jobs.UpdateJobStatus(ctx, job.ResourceInstanceID, s.id, domain.JobStatusFailed)
		if err == nil && updated {
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
					return nil
				case msg := <-recvStream:

					if _, ok := msg.Payload.(*convergeplanev1.WorkerMessage_Ready); !ok {
						return errors.New("protocol error") // protocol error
					}

					for {
						resourceInstID, err := s.jobs.GetAvailableJob(stream.Context())
						if resourceInstID == nil || err != nil {
							select {
							case <-stream.Context().Done():
								return nil
							case <-time.After(jobLookupInterval):
								continue
							}
						}

						job, err := s.jobs.AcquireJob(stream.Context(), *resourceInstID, s.id, leaseTime)
						if job == nil || err != nil {
							select {
							case <-stream.Context().Done():
								return nil
							case <-time.After(jobLookupInterval):
								continue
							}
						}

						// periodically re-up the lease
						go func() {
							ticker := time.NewTicker(leaseWake)
							for {
								select {
								case <-cancelLease:
									s.jobs.ReleaseJob(context.Background(), *resourceInstID, s.id)
									return
								case <-ticker.C:
									renewed, err := s.jobs.RenewJobLease(stream.Context(), *resourceInstID, s.id, leaseTime)
									if !renewed || err != nil {
										close(leaseExpired)
										return
									}
									ticker.Reset(leaseWake)
								}
							}
						}()

						contextStruct, err := structpb.NewStruct(job.Context)
						if err != nil {
							cancelLeaseIfExists(cancelLease)
							select {
							case <-stream.Context().Done():
								return nil
							case <-time.After(jobLookupInterval):
								continue
							}
						}

						err = stream.Send(&convergeplanev1.ServerCommand{
							Payload: &convergeplanev1.ServerCommand_Launch{
								Launch: &convergeplanev1.LaunchOperation{
									Operation: &convergeplanev1.AtomicOperation{
										Id:            job.RequestID,
										ResourceId:    job.ResourceInstanceID,
										OperationType: job.JobType,
										Context:       contextStruct,
										Name:          job.CurrentAtomicOperation,
									},
								},
							},
						})
						if err != nil {
							return err
						}

						currentJob = job
						break
					}
				}
				state = ConnectedWaiting

			case ConnectedWaiting:
				select {
				case <-stream.Context().Done():
					return nil
				case <-time.After(clientResponseTime):
					err := cancelOperation(currentJob.RequestID)
					if err != nil {
						return err
					}
					return nil
				case ack := <-recvStream:
					update, ok := ack.Payload.(*convergeplanev1.WorkerMessage_Update)
					if !ok {
						return errors.New("protocol error") // protocol error
					}
					if update.Update.OperationId != currentJob.RequestID {
						return errors.New("wrong operation id from client")
					}
					if update.Update.Result.Status != convergeplanev1.OperationStatus_OPERATION_STATUS_IN_PROGRESS {
						return errors.New("unexpected status from client")
					}
					updated, err := s.jobs.UpdateJobStatus(stream.Context(), currentJob.ResourceInstanceID, s.id, domain.JobStatusRunning)
					if err != nil {
						return err
					}
					if !updated {
						return nil
					}
				}
				state = ConnectedBusy

			case ConnectedBusy:
				ticker := time.NewTicker(time.Duration(s.defaultJobTimeout) * time.Second)

			ConnectedBusyLoop:
				for {
					select {
					case <-stream.Context().Done():
						failOperation(cancelLease, currentJob)
						return nil
					case msg := <-recvStream:
						update, ok := msg.Payload.(*convergeplanev1.WorkerMessage_Update)
						if !ok {
							failOperation(cancelLease, currentJob)
							return errors.New("protocol error") // protocol error
						}
						if update.Update.OperationId != currentJob.RequestID {
							failOperation(cancelLease, currentJob)
							return errors.New("wrong operation id from client")
						}
						switch update.Update.Result.Status {
						case convergeplanev1.OperationStatus_OPERATION_STATUS_COMPLETED:
							completeOperation(cancelLease, currentJob, update.Update.Result)
						case convergeplanev1.OperationStatus_OPERATION_STATUS_FAILED:
							failOperation(cancelLease, currentJob)
						case convergeplanev1.OperationStatus_OPERATION_STATUS_IN_PROGRESS:
							continue ConnectedBusyLoop
						case convergeplanev1.OperationStatus_OPERATION_STATUS_CANCELLED: // This is unexpected
							failOperation(cancelLease, currentJob)
						}
						state = ConnectedIdle
						break ConnectedBusyLoop
					case <-ticker.C:
						err := cancelOperation(currentJob.RequestID)
						if err != nil {
							failOperation(cancelLease, currentJob)
							return err
						}
						state = CancelRequested
						break ConnectedBusyLoop
					}
				}

			case CancelRequested:
				ticker := time.NewTicker(clientResponseTime)

			CancelLoop:
				for {
					select {
					case <-stream.Context().Done():
						failOperation(cancelLease, currentJob)
						return nil
					case <-ticker.C:
						failOperation(cancelLease, currentJob)
						return errors.New("cancel ack timed out")
					case msg := <-recvStream:
						ack, ok := msg.Payload.(*convergeplanev1.WorkerMessage_Update)
						if !ok {
							failOperation(cancelLease, currentJob)
							return errors.New("protocol error")
						} else if ack.Update.OperationId != currentJob.RequestID {
							failOperation(cancelLease, currentJob)
							return errors.New("wrong operation id")
						}
						switch ack.Update.Result.Status {
						case convergeplanev1.OperationStatus_OPERATION_STATUS_COMPLETED:
							completeOperation(cancelLease, currentJob, ack.Update.Result)
						case convergeplanev1.OperationStatus_OPERATION_STATUS_FAILED,
							convergeplanev1.OperationStatus_OPERATION_STATUS_CANCELLED:
							failOperation(cancelLease, currentJob)
						case convergeplanev1.OperationStatus_OPERATION_STATUS_IN_PROGRESS:
							continue
						}
						break CancelLoop
					}
				}
				state = ConnectedIdle
			}
		}
	}()

	select {
	case <-leaseExpired:
	case <-stream.Context().Done():
	case <-controlCodeCancelled:
	}

	return nil
}
