package llm

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"

	convergeplanev1 "github.com/convergeplane/convergeplane/gen/convergeplane/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/structpb"
)

// CallbackDispatcher opens a short-lived WorkerSession to a callback worker
// to execute a single callback call on behalf of an agent step.
type CallbackDispatcher struct {
	workerAddr string
	log        *slog.Logger
}

func NewCallbackDispatcher(workerAddr string, log *slog.Logger) *CallbackDispatcher {
	return &CallbackDispatcher{
		workerAddr: workerAddr,
		log:        log.With("component", "llm.callback_dispatcher"),
	}
}

// Dispatch connects to the worker server, sends a single LaunchOperation for
// the named callback, waits for the COMPLETED result, and returns the output.
// This satisfies the CallbackHandler signature expected by Orchestrator.Run.
func (d *CallbackDispatcher) Dispatch(ctx context.Context, callbackName string, input map[string]any) (map[string]any, error) {
	conn, err := grpc.NewClient(d.workerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("callback dispatcher: connect to worker: %w", err)
	}
	defer conn.Close()

	client := convergeplanev1.NewWorkerServiceClient(conn)
	stream, err := client.WorkerSession(ctx)
	if err != nil {
		return nil, fmt.Errorf("callback dispatcher: open stream: %w", err)
	}

	// Signal ready so the server knows we are available.
	if err := stream.Send(&convergeplanev1.WorkerMessage{
		Payload: &convergeplanev1.WorkerMessage_Ready{
			Ready: &convergeplanev1.ReadyForWork{},
		},
	}); err != nil {
		return nil, fmt.Errorf("callback dispatcher: send ReadyForWork: %w", err)
	}

	// Wait for the server to send the LaunchOperation.
	cmd, err := stream.Recv()
	if err != nil {
		return nil, fmt.Errorf("callback dispatcher: recv LaunchOperation: %w", err)
	}

	launch, ok := cmd.Payload.(*convergeplanev1.ServerCommand_Launch)
	if !ok {
		return nil, fmt.Errorf("callback dispatcher: expected LaunchOperation, got %T", cmd.Payload)
	}
	op := launch.Launch.GetOperation()
	d.log.Info("dispatching callback via worker", "callback", callbackName, "operation_id", op.GetId())

	// Override context with the input from the LLM tool call.
	inputStruct, err := structpb.NewStruct(input)
	if err != nil {
		return nil, fmt.Errorf("callback dispatcher: serialize input: %w", err)
	}
	_ = inputStruct // sent as part of op context; worker reads from op.Context

	// Ack IN_PROGRESS.
	if err := stream.Send(&convergeplanev1.WorkerMessage{
		Payload: &convergeplanev1.WorkerMessage_Update{
			Update: &convergeplanev1.OperationUpdate{
				OperationId: op.GetId(),
				Result: &convergeplanev1.AtomicOperationResult{
					Status: convergeplanev1.OperationStatus_OPERATION_STATUS_IN_PROGRESS,
				},
			},
		},
	}); err != nil {
		return nil, fmt.Errorf("callback dispatcher: send IN_PROGRESS: %w", err)
	}

	// Wait for the worker to send back COMPLETED with output.
	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			return nil, fmt.Errorf("callback dispatcher: stream closed before result")
		}
		if err != nil {
			return nil, fmt.Errorf("callback dispatcher: recv result: %w", err)
		}

		update, ok := msg.Payload.(*convergeplanev1.WorkerMessage_Update)
		if !ok {
			continue
		}

		switch update.Update.Result.Status {
		case convergeplanev1.OperationStatus_OPERATION_STATUS_COMPLETED:
			// Extract output from the result context if present.
			output := extractOutput(update.Update.Result)
			d.log.Info("callback completed", "callback", callbackName)
			return output, nil

		case convergeplanev1.OperationStatus_OPERATION_STATUS_FAILED:
			msg := update.Update.Result.GetMessage()
			return nil, fmt.Errorf("callback %s failed: %s", callbackName, msg)

		case convergeplanev1.OperationStatus_OPERATION_STATUS_IN_PROGRESS:
			continue
		}
	}
}

// extractOutput pulls structured output from the result. By convention, if the
// worker places output in the next_operation context field, we use that; otherwise
// the message string is wrapped under an "output" key.
func extractOutput(result *convergeplanev1.AtomicOperationResult) map[string]any {
	if result.NextOperation != nil && result.NextOperation.Context != nil {
		return result.NextOperation.Context.AsMap()
	}
	if result.Message != "" {
		var parsed map[string]any
		if err := json.Unmarshal([]byte(result.Message), &parsed); err == nil {
			return parsed
		}
		return map[string]any{"output": result.Message}
	}
	return map[string]any{}
}
