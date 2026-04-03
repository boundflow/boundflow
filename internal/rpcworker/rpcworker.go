package rpcworker

import (
	convergeplanev1 "github.com/convergeplane/convergeplane/gen/convergeplane/v1"
	"google.golang.org/grpc"
)

type RpcWorker struct {
	convergeplanev1.UnimplementedWorkerServiceServer
}

type State int

const (
	ConnectedIdle = iota
	ConnectedBusy
	CancelRequested
)

func NewRpcWorker() *RpcWorker {
	return &RpcWorker{}
}

// The RpcWorker goes through the following state machine:
//
// ConnectedIdle
//
//	-> LaunchOperation sent
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

	state := ConnectedIdle

	for {
		msg, err := stream.Recv()

		if state == ConnectedIdle {

			if err != nil {
				return err
			}

			if _, ok := msg.Payload.(*convergeplanev1.WorkerMessage_Ready); ok {

				stream.Send(&convergeplanev1.ServerCommand{
					Payload: &convergeplanev1.ServerCommand_Launch{},
				})
			}
		}
	}

}
