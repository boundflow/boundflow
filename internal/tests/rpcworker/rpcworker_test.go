package rpcworker_test

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	convergeplanev1 "github.com/convergeplane/convergeplane/gen/convergeplane/v1"
	"github.com/convergeplane/convergeplane/internal/domain"
	"github.com/convergeplane/convergeplane/internal/rpcworker"
	"github.com/convergeplane/convergeplane/internal/storage/mocks"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc/metadata"
)

// ---- stream mock ----

type recvResult struct {
	msg *convergeplanev1.WorkerMessage
	err error
}

type mockStream struct {
	ctx    context.Context
	recvCh chan recvResult
	sendCh chan *convergeplanev1.ServerCommand
}

func newMockStream(ctx context.Context) *mockStream {
	return &mockStream{
		ctx:    ctx,
		recvCh: make(chan recvResult, 10),
		sendCh: make(chan *convergeplanev1.ServerCommand, 10),
	}
}

func (m *mockStream) Recv() (*convergeplanev1.WorkerMessage, error) {
	select {
	case r := <-m.recvCh:
		return r.msg, r.err
	case <-m.ctx.Done():
		return nil, m.ctx.Err()
	}
}

func (m *mockStream) Send(cmd *convergeplanev1.ServerCommand) error {
	m.sendCh <- cmd
	return nil
}

func (m *mockStream) Context() context.Context     { return m.ctx }
func (m *mockStream) SetHeader(metadata.MD) error  { return nil }
func (m *mockStream) SendHeader(metadata.MD) error { return nil }
func (m *mockStream) SetTrailer(metadata.MD)       {}
func (m *mockStream) SendMsg(any) error            { return nil }
func (m *mockStream) RecvMsg(any) error            { return nil }

func (m *mockStream) push(msg *convergeplanev1.WorkerMessage) {
	m.recvCh <- recvResult{msg: msg}
}

// ---- message helpers ----

func readyMsg() *convergeplanev1.WorkerMessage {
	return &convergeplanev1.WorkerMessage{
		Payload: &convergeplanev1.WorkerMessage_Ready{
			Ready: &convergeplanev1.ReadyForWork{},
		},
	}
}

func updateMsg(opID string, status convergeplanev1.OperationStatus) *convergeplanev1.WorkerMessage {
	return &convergeplanev1.WorkerMessage{
		Payload: &convergeplanev1.WorkerMessage_Update{
			Update: &convergeplanev1.OperationUpdate{
				OperationId: opID,
				Result: &convergeplanev1.AtomicOperationResult{
					Status: status,
				},
			},
		},
	}
}

// ---- scheduler mock ----

type mockScheduler struct {
	completeCh chan string
	failCh     chan string
}

func newMockScheduler() *mockScheduler {
	return &mockScheduler{
		completeCh: make(chan string, 1),
		failCh:     make(chan string, 1),
	}
}

func (m *mockScheduler) CompleteRequest(_ context.Context, req string) (bool, error) {
	m.completeCh <- req
	return true, nil
}

func (m *mockScheduler) FailRequest(_ context.Context, req string) (bool, error) {
	m.failCh <- req
	return true, nil
}

// ---- constants and helpers ----

const (
	testWorkerID   = "test-worker"
	testResourceID = "resource-1"
	testRequestID  = "req-1"
)

func newTestWorker(ctrl *gomock.Controller) (*rpcworker.RpcWorker, *mocks.MockJobRepository, *mockScheduler) {
	jobRepo := mocks.NewMockJobRepository(ctrl)
	sched := newMockScheduler()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	return rpcworker.NewRpcWorker(jobRepo, testWorkerID, 60, sched, log), jobRepo, sched
}

func testJob() *domain.Job {
	return &domain.Job{
		ResourceInstanceID:     testResourceID,
		RequestID:              testRequestID,
		CurrentAtomicOperation: "create",
		JobType:                "create",
		Context:                map[string]any{},
		Policy:                 domain.JobPolicy{OperationTimeoutSeconds: 60},
	}
}

func runSession(worker *rpcworker.RpcWorker, stream *mockStream) chan error {
	ch := make(chan error, 1)
	go func() { ch <- worker.WorkerSession(stream) }()
	return ch
}

// expectJobAcquired sets up expectations for finding and acquiring a job.
// RenewJobLease and ReleaseJob are AnyTimes since they're managed by the lease
// goroutine and are hard to time deterministically relative to the test.
func expectJobAcquired(jobRepo *mocks.MockJobRepository) {
	resID := testResourceID
	jobRepo.EXPECT().GetAvailableJob(gomock.Any()).Return(&resID, nil)
	jobRepo.EXPECT().AcquireJob(gomock.Any(), testResourceID, testWorkerID, gomock.Any()).Return(testJob(), nil)
	jobRepo.EXPECT().RenewJobLease(gomock.Any(), testResourceID, testWorkerID, gomock.Any()).Return(true, nil).AnyTimes()
	jobRepo.EXPECT().ReleaseJob(gomock.Any(), testResourceID, testWorkerID).Return(nil).AnyTimes()
}

// driveToConnectedBusy sets up the UpdateJobStatus(running) expectation, pushes the
// initial ReadyForWork and IN_PROGRESS messages, reads the LaunchOperation from the
// stream, and blocks until the inner goroutine has confirmed running state (and is
// therefore in ConnectedBusy). WorkerSession must be running before calling this.
func driveToConnectedBusy(t *testing.T, jobRepo *mocks.MockJobRepository, stream *mockStream) {
	t.Helper()

	runningSet := make(chan struct{})
	jobRepo.EXPECT().
		UpdateJobStatus(gomock.Any(), testResourceID, testWorkerID, domain.JobStatusRunning).
		DoAndReturn(func(_ context.Context, _, _ string, _ domain.JobStatus) (bool, error) {
			close(runningSet)
			return true, nil
		})

	stream.push(readyMsg())

	launch := <-stream.sendCh
	if launch.GetLaunch() == nil {
		t.Fatal("expected LaunchOperation from server")
	}

	stream.push(updateMsg(testRequestID, convergeplanev1.OperationStatus_OPERATION_STATUS_IN_PROGRESS))
	<-runningSet
}

// assertReceived asserts that a string is sent to ch within a 2-second timeout.
func assertReceived(t *testing.T, ch chan string, want, label string) {
	t.Helper()
	select {
	case got := <-ch:
		if got != want {
			t.Errorf("%s: got %q, want %q", label, got, want)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("%s not called within timeout", label)
	}
}

// ---- Tests ----

// ConnectedIdle: first message is not ReadyForWork → protocol error.
func TestWorkerSession_ProtocolError_NonReadyInIdle(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	worker, _, _ := newTestWorker(ctrl)
	stream := newMockStream(ctx)
	errCh := runSession(worker, stream)

	stream.push(updateMsg(testRequestID, convergeplanev1.OperationStatus_OPERATION_STATUS_IN_PROGRESS))

	// WorkerSession returns nil (inner goroutine error is not propagated), but the
	// session should terminate and no job repo calls should be made.
	select {
	case <-errCh:
	case <-time.After(2 * time.Second):
		t.Fatal("WorkerSession did not return in time")
	}
}

// ConnectedIdle: stream context cancelled before any message → clean nil return.
func TestWorkerSession_StreamDisconnect_BeforeReady(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctx, cancel := context.WithCancel(context.Background())

	worker, _, _ := newTestWorker(ctrl)
	stream := newMockStream(ctx)
	errCh := runSession(worker, stream)

	cancel()

	if err := <-errCh; err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
}

// ConnectedIdle: no job available, then stream disconnects → GetAvailableJob called once, clean return.
func TestWorkerSession_NoJob_StreamDisconnects(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctx, cancel := context.WithCancel(context.Background())

	worker, jobRepo, _ := newTestWorker(ctrl)

	called := make(chan struct{})
	jobRepo.EXPECT().GetAvailableJob(gomock.Any()).
		DoAndReturn(func(context.Context) (*string, error) {
			close(called)
			return nil, nil
		})

	stream := newMockStream(ctx)
	errCh := runSession(worker, stream)

	stream.push(readyMsg())
	<-called
	cancel()

	if err := <-errCh; err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
}

// ConnectedIdle: AcquireJob returns nil (race lost to another worker) → retries, then stream disconnects.
func TestWorkerSession_AcquireJob_Fails_StreamDisconnects(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctx, cancel := context.WithCancel(context.Background())

	worker, jobRepo, _ := newTestWorker(ctrl)

	resID := testResourceID
	jobRepo.EXPECT().GetAvailableJob(gomock.Any()).Return(&resID, nil)

	acquired := make(chan struct{})
	jobRepo.EXPECT().AcquireJob(gomock.Any(), testResourceID, testWorkerID, gomock.Any()).
		DoAndReturn(func(_ context.Context, _, _ string, _ time.Duration) (*domain.Job, error) {
			close(acquired)
			return nil, nil // failed to acquire
		})

	stream := newMockStream(ctx)
	errCh := runSession(worker, stream)

	stream.push(readyMsg())
	<-acquired
	cancel()

	if err := <-errCh; err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
}

// Full happy path: ReadyForWork → LaunchOperation → IN_PROGRESS → COMPLETED → CompleteRequest.
func TestWorkerSession_CompleteOperation(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	worker, jobRepo, sched := newTestWorker(ctrl)
	expectJobAcquired(jobRepo)
	jobRepo.EXPECT().UpdateJobStatus(gomock.Any(), testResourceID, testWorkerID, domain.JobStatusRunning).Return(true, nil)
	jobRepo.EXPECT().UpdateJobStatus(gomock.Any(), testResourceID, testWorkerID, domain.JobStatusCompleted).Return(true, nil)

	stream := newMockStream(ctx)
	errCh := runSession(worker, stream)

	stream.push(readyMsg())

	launch := <-stream.sendCh
	if launch.GetLaunch() == nil {
		t.Fatal("expected LaunchOperation")
	}
	if got := launch.GetLaunch().GetOperation().GetId(); got != testRequestID {
		t.Errorf("operation id: got %q, want %q", got, testRequestID)
	}

	stream.push(updateMsg(testRequestID, convergeplanev1.OperationStatus_OPERATION_STATUS_IN_PROGRESS))
	stream.push(updateMsg(testRequestID, convergeplanev1.OperationStatus_OPERATION_STATUS_COMPLETED))

	assertReceived(t, sched.completeCh, testRequestID, "CompleteRequest")

	cancel()
	if err := <-errCh; err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
}

// Full path with failure: ReadyForWork → LaunchOperation → IN_PROGRESS → FAILED → FailRequest.
func TestWorkerSession_FailOperation(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	worker, jobRepo, sched := newTestWorker(ctrl)
	expectJobAcquired(jobRepo)
	jobRepo.EXPECT().UpdateJobStatus(gomock.Any(), testResourceID, testWorkerID, domain.JobStatusRunning).Return(true, nil)
	jobRepo.EXPECT().UpdateJobStatus(gomock.Any(), testResourceID, testWorkerID, domain.JobStatusFailed).Return(true, nil)

	stream := newMockStream(ctx)
	errCh := runSession(worker, stream)

	stream.push(readyMsg())
	<-stream.sendCh // LaunchOperation
	stream.push(updateMsg(testRequestID, convergeplanev1.OperationStatus_OPERATION_STATUS_IN_PROGRESS))
	stream.push(updateMsg(testRequestID, convergeplanev1.OperationStatus_OPERATION_STATUS_FAILED))

	assertReceived(t, sched.failCh, testRequestID, "FailRequest")

	cancel()
	if err := <-errCh; err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
}

// ConnectedBusy: stream disconnects → failOperation called → FailRequest.
func TestWorkerSession_ConnectedBusy_StreamDisconnect(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctx, cancel := context.WithCancel(context.Background())

	worker, jobRepo, sched := newTestWorker(ctrl)
	expectJobAcquired(jobRepo)
	jobRepo.EXPECT().UpdateJobStatus(gomock.Any(), testResourceID, testWorkerID, domain.JobStatusFailed).Return(true, nil)

	stream := newMockStream(ctx)
	errCh := runSession(worker, stream)

	driveToConnectedBusy(t, jobRepo, stream)

	cancel()

	assertReceived(t, sched.failCh, testRequestID, "FailRequest")

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("expected nil, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("WorkerSession did not return in time")
	}
}

// ConnectedWaiting: client sends wrong operation ID → session terminates, no status updates made.
func TestWorkerSession_ConnectedWaiting_WrongOperationId(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	worker, jobRepo, _ := newTestWorker(ctrl)
	expectJobAcquired(jobRepo)

	stream := newMockStream(ctx)
	errCh := runSession(worker, stream)

	stream.push(readyMsg())
	<-stream.sendCh // LaunchOperation

	stream.push(updateMsg("wrong-op-id", convergeplanev1.OperationStatus_OPERATION_STATUS_IN_PROGRESS))

	select {
	case <-errCh:
	case <-time.After(2 * time.Second):
		t.Fatal("WorkerSession did not return in time")
	}
}

// ConnectedWaiting: client sends unexpected status (not IN_PROGRESS) → session terminates.
func TestWorkerSession_ConnectedWaiting_UnexpectedStatus(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	worker, jobRepo, _ := newTestWorker(ctrl)
	expectJobAcquired(jobRepo)

	stream := newMockStream(ctx)
	errCh := runSession(worker, stream)

	stream.push(readyMsg())
	<-stream.sendCh // LaunchOperation

	// Send COMPLETED before IN_PROGRESS — unexpected
	stream.push(updateMsg(testRequestID, convergeplanev1.OperationStatus_OPERATION_STATUS_COMPLETED))

	select {
	case <-errCh:
	case <-time.After(2 * time.Second):
		t.Fatal("WorkerSession did not return in time")
	}
}

// ConnectedBusy: client sends wrong operation ID → failOperation called, session terminates.
func TestWorkerSession_ConnectedBusy_WrongOperationId(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	worker, jobRepo, sched := newTestWorker(ctrl)
	expectJobAcquired(jobRepo)
	jobRepo.EXPECT().UpdateJobStatus(gomock.Any(), testResourceID, testWorkerID, domain.JobStatusFailed).Return(true, nil)

	stream := newMockStream(ctx)
	errCh := runSession(worker, stream)

	driveToConnectedBusy(t, jobRepo, stream)

	stream.push(updateMsg("wrong-op-id", convergeplanev1.OperationStatus_OPERATION_STATUS_COMPLETED))

	assertReceived(t, sched.failCh, testRequestID, "FailRequest")

	select {
	case <-errCh:
	case <-time.After(2 * time.Second):
		t.Fatal("WorkerSession did not return in time")
	}
}
