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
// ConnectedIdle => DONE
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
// CancelRequested => DONE
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

	go func() {
		for {
			msg, err := stream.Recv()
			if err != nil {
				return
			}
			recvStream <- msg
		}
	}()

	cancelOperation := func(cancelLease chan bool, operationId string) error {
		cancelLease <- true
		return stream.Send(&convergeplanev1.ServerCommand{
			Payload: &convergeplanev1.ServerCommand_Cancel{
				Cancel: &convergeplanev1.CancelOperation{
					OperationId: operationId,
				},
			},
		})
	}

	completeOperation := func(job *domain.Job, result *convergeplanev1.AtomicOperationResult) error {
		// either complete the parent request if its the final one, or schedule the next operation

		// request completion doesnt depend on stream context
		if result.NextOperation == nil {
			err := s.jobs.UpdateJobStatus(context.Background(), job.ResourceInstanceID, s.id, domain.JobStatusCompleted); if err != nil {
				return err
			}
			s.scheduler.CompleteRequest(context.Background(), job.RequestID) // Can fire and forget here since scheduler will deal with it later
		}
		else {
			// mark the current request as open and give up the lease
			
			

		}

	}

	failOperation := func() error {
		// put the request into failed state
		return errors.ErrUnsupported
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
										leaseExpired <- true
										return
									}
									ticker.Reset(leaseWake)
								}
							}
						}()

						contextStruct, err := structpb.NewStruct(job.Context)
						if err != nil {
							cancelLease <- true
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
										Name:          "Entry_Operation"
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

			case ConnectedBusy:
				ticker := time.NewTicker(time.Duration(s.defaultJobTimeout) * time.Second)

			ConnectedLoop:
				for {
					select {
					case <-stream.Context().Done():
						failOperation()
						return nil
					case msg := <-recvStream:
						update, ok := msg.Payload.(*convergeplanev1.WorkerMessage_Update)
						if !ok {
							return errors.New("protocol error") // protocol error
						}
						if update.Update.OperationId != currentJob.RequestID {
							return errors.New("wrong operation id from client")
						}
						switch update.Update.Result.Status {
						case convergeplanev1.OperationStatus_OPERATION_STATUS_COMPLETED:
							completeOperation(currentJob, update.Update.Result)
						case convergeplanev1.OperationStatus_OPERATION_STATUS_FAILED:
							failOperation()
						case convergeplanev1.OperationStatus_OPERATION_STATUS_IN_PROGRESS:
							continue ConnectedLoop
						case convergeplanev1.OperationStatus_OPERATION_STATUS_CANCELLED: // This is unexpected
							failOperation()
						}
						state = ConnectedIdle
						break ConnectedLoop
					case <-ticker.C:
						err := cancelOperation(cancelLease, currentJob.RequestID)
						if err != nil {
							failOperation()
							return err
						}
						state = CancelRequested
						break ConnectedLoop
					}
				}

			case CancelRequested:
				ticker := time.NewTicker(clientResponseTime)

			CancelLoop:
				for {
					select {
					case <-stream.Context().Done():
						failOperation()
						return nil
					case <-ticker.C:
						return errors.New("cancel ack timed out")
					case msg := <-recvStream:
						ack, ok := msg.Payload.(*convergeplanev1.WorkerMessage_Update)
						if !ok {
							return errors.New("protocol error")
						} else if ack.Update.OperationId != currentJob.RequestID {
							return errors.New("wrong operation id")
						}
						switch ack.Update.Result.Status {
						case convergeplanev1.OperationStatus_OPERATION_STATUS_COMPLETED:
							completeOperation()
						case convergeplanev1.OperationStatus_OPERATION_STATUS_FAILED,
							convergeplanev1.OperationStatus_OPERATION_STATUS_CANCELLED:
							failOperation()
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
	}

	return nil
}
