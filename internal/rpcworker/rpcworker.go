package rpcworker

import (
	"context"
	"time"

	convergeplanev1 "github.com/convergeplane/convergeplane/gen/convergeplane/v1"
	"github.com/convergeplane/convergeplane/internal/storage"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/structpb"
)

type RpcWorker struct {
	convergeplanev1.UnimplementedWorkerServiceServer
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

func NewRpcWorker(jobs storage.JobRepository, id string, jobTimeout int) *RpcWorker {
	return &RpcWorker{
		jobs:              jobs,
		id:                id,
		defaultJobTimeout: jobTimeout,
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
	cancelLease := make(chan bool)
	recvStream := make(chan *convergeplanev1.WorkerMessage)

	defer close(cancelLease)

	//resetState := func() {

	//}

	go func() {
		for {
			msg, err := stream.Recv()
			if err != nil {
				return
			}
			recvStream <- msg
		}
	}()

	go func() error {

		state := ConnectedIdle
		leaseWake := 4 * time.Second
		leaseTime := leaseWake + 1*time.Second
		jobLookupInterval := 5 * time.Second
		var operationId string

		for {
		stateEval:
			switch state {

			case ConnectedIdle:
				select {
				case <-stream.Context().Done():
					return nil

				case msg := <-recvStream:

					if _, ok := msg.Payload.(*convergeplanev1.WorkerMessage_Ready); !ok {
						state = ConnectedIdle
						break stateEval
					}

					for {
						resourceInstID, err := s.jobs.GetAvailableJob(stream.Context())
						if resourceInstID == nil || err != nil {
							time.Sleep(jobLookupInterval)
							continue
						}

						job, err := s.jobs.AcquireJob(stream.Context(), *resourceInstID, s.id, leaseTime)
						if job == nil || err != nil {
							time.Sleep(jobLookupInterval)
							continue
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
							time.Sleep(jobLookupInterval)
							cancelLease <- true
							continue
						}
						err = stream.Send(&convergeplanev1.ServerCommand{
							Payload: &convergeplanev1.ServerCommand_Launch{
								Launch: &convergeplanev1.LaunchOperation{
									Operation: &convergeplanev1.AtomicOperation{
										Id:            job.RequestID,
										ResourceId:    job.ResourceInstanceID,
										OperationType: job.JobType,
										Context:       contextStruct,
									},
								},
							},
						})

						if err != nil {
							return err
						}

						operationId = job.RequestID

						state = ConnectedWaiting
						break
					}
				}

			case ConnectedWaiting:
				select {
				case <-stream.Context().Done():
					return nil

				case msg := <-recvStream:

					update, ok := msg.Payload.(*convergeplanev1.WorkerMessage_Update)

					if !ok {
						state = ConnectedIdle
						cancelLease <- true
						break stateEval
					}

					if update.Update.OperationId != operationId {
						state = ConnectedIdle
						cancelLease <- true
						break stateEval
					}

				}
			}
		}
	}()

	select {
	case <-leaseExpired:
	case <-stream.Context().Done():
	}

	return nil
}
