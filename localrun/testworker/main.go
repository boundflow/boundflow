// testworker is a local E2E test client that simulates a worker SDK.
// It connects to the WorkerService, accepts operations, and immediately
// marks them as completed so the full control-plane flow can be exercised.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"time"

	boundflowv1 "github.com/boundflow/boundflow/gen/boundflow/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	addr := flag.String("addr", "localhost:50052", "worker gRPC server address")
	flag.Parse()

	log := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	log.Info("test worker starting", "addr", *addr)

	conn, err := grpc.NewClient(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Error("failed to connect", "error", err)
		os.Exit(1)
	}
	defer conn.Close()

	client := boundflowv1.NewWorkerServiceClient(conn)

	for {
		if err := runSession(client, log); err != nil {
			log.Error("session ended with error, retrying in 3s", "error", err)
			time.Sleep(3 * time.Second)
		} else {
			log.Info("session ended cleanly, reconnecting in 1s")
			time.Sleep(1 * time.Second)
		}
	}
}

func runSession(client boundflowv1.WorkerServiceClient, log *slog.Logger) error {
	ctx := context.Background()

	stream, err := client.WorkerSession(ctx)
	if err != nil {
		return fmt.Errorf("open stream: %w", err)
	}

	log.Info("stream opened, sending ReadyForWork")
	if err := stream.Send(&boundflowv1.WorkerMessage{
		Payload: &boundflowv1.WorkerMessage_Ready{
			Ready: &boundflowv1.ReadyForWork{},
		},
	}); err != nil {
		return fmt.Errorf("send ReadyForWork: %w", err)
	}

	for {
		cmd, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return fmt.Errorf("recv: %w", err)
		}

		switch p := cmd.Payload.(type) {
		case *boundflowv1.ServerCommand_Launch:
			op := p.Launch.GetOperation()
			log.Info("received LaunchOperation",
				"operation_id", op.GetId(),
				"resource_id", op.GetResourceId(),
				"operation_type", op.GetOperationType(),
				"name", op.GetName(),
				"context", op.GetContext(),
			)

			// Ack with IN_PROGRESS
			if err := stream.Send(&boundflowv1.WorkerMessage{
				Payload: &boundflowv1.WorkerMessage_Update{
					Update: &boundflowv1.OperationUpdate{
						OperationId: op.GetId(),
						Result: &boundflowv1.AtomicOperationResult{
							Status: boundflowv1.OperationStatus_OPERATION_STATUS_IN_PROGRESS,
						},
					},
				},
			}); err != nil {
				return fmt.Errorf("send IN_PROGRESS: %w", err)
			}
			sleepDuration := time.Duration(op.GetTimeoutSeconds()) * time.Second
			log.Info("sent IN_PROGRESS, simulating work", "timeout_seconds", op.GetTimeoutSeconds(), "sleeping", sleepDuration)
			time.Sleep(sleepDuration)

			// Mark complete
			if err := stream.Send(&boundflowv1.WorkerMessage{
				Payload: &boundflowv1.WorkerMessage_Update{
					Update: &boundflowv1.OperationUpdate{
						OperationId: op.GetId(),
						Result: &boundflowv1.AtomicOperationResult{
							Status: boundflowv1.OperationStatus_OPERATION_STATUS_COMPLETED,
						},
					},
				},
			}); err != nil {
				return fmt.Errorf("send COMPLETED: %w", err)
			}
			log.Info("sent COMPLETED, signalling ready for next job", "operation_id", op.GetId())
			if err := stream.Send(&boundflowv1.WorkerMessage{
				Payload: &boundflowv1.WorkerMessage_Ready{
					Ready: &boundflowv1.ReadyForWork{},
				},
			}); err != nil {
				return fmt.Errorf("send ReadyForWork: %w", err)
			}

		case *boundflowv1.ServerCommand_Cancel:
			opID := p.Cancel.GetOperationId()
			log.Warn("received CancelOperation, acking with CANCELLED", "operation_id", opID)
			if err := stream.Send(&boundflowv1.WorkerMessage{
				Payload: &boundflowv1.WorkerMessage_Update{
					Update: &boundflowv1.OperationUpdate{
						OperationId: opID,
						Result: &boundflowv1.AtomicOperationResult{
							Status: boundflowv1.OperationStatus_OPERATION_STATUS_CANCELLED,
						},
					},
				},
			}); err != nil {
				return fmt.Errorf("send CANCELLED: %w", err)
			}
			if err := stream.Send(&boundflowv1.WorkerMessage{
				Payload: &boundflowv1.WorkerMessage_Ready{
					Ready: &boundflowv1.ReadyForWork{},
				},
			}); err != nil {
				return fmt.Errorf("send ReadyForWork after cancel: %w", err)
			}
		}
	}
}
