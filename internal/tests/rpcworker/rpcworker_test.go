package rpcworker_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	boundflowv1 "github.com/boundflow/boundflow/gen/boundflow/v1"
	"github.com/boundflow/boundflow/internal/auth"
	"github.com/boundflow/boundflow/internal/domain"
	"github.com/boundflow/boundflow/internal/rpcworker"
	"github.com/boundflow/boundflow/internal/storage/mocks"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc/metadata"
)

// ---- stream mock ----

type recvResult struct {
	msg *boundflowv1.WorkerMessage
	err error
}

type mockStream struct {
	ctx     context.Context
	recvCh  chan recvResult
	sendCh  chan *boundflowv1.ServerCommand
	sendErr error // when set (before the session starts), Send returns this instead of queuing
}

func newMockStream(ctx context.Context) *mockStream {
	return &mockStream{
		ctx:    auth.WithTenantGroup(ctx, testTenantGroupID),
		recvCh: make(chan recvResult, 10),
		sendCh: make(chan *boundflowv1.ServerCommand, 10),
	}
}

func (m *mockStream) Recv() (*boundflowv1.WorkerMessage, error) {
	select {
	case r := <-m.recvCh:
		return r.msg, r.err
	case <-m.ctx.Done():
		return nil, m.ctx.Err()
	}
}

func (m *mockStream) Send(cmd *boundflowv1.ServerCommand) error {
	if m.sendErr != nil {
		return m.sendErr
	}
	m.sendCh <- cmd
	return nil
}

func (m *mockStream) Context() context.Context     { return m.ctx }
func (m *mockStream) SetHeader(metadata.MD) error  { return nil }
func (m *mockStream) SendHeader(metadata.MD) error { return nil }
func (m *mockStream) SetTrailer(metadata.MD)       {}
func (m *mockStream) SendMsg(any) error            { return nil }
func (m *mockStream) RecvMsg(any) error            { return nil }

func (m *mockStream) push(msg *boundflowv1.WorkerMessage) {
	m.recvCh <- recvResult{msg: msg}
}

// ---- message helpers ----

func readyMsg() *boundflowv1.WorkerMessage {
	return &boundflowv1.WorkerMessage{
		Payload: &boundflowv1.WorkerMessage_Ready{
			Ready: &boundflowv1.ReadyForWork{
				Capabilities: []*boundflowv1.WorkerCapability{
					{WorkflowType: testWorkflowType, WorkflowVersion: testWorkflowVersion},
				},
			},
		},
	}
}

func updateMsg(opID string, status boundflowv1.OperationStatus) *boundflowv1.WorkerMessage {
	return &boundflowv1.WorkerMessage{
		Payload: &boundflowv1.WorkerMessage_Update{
			Update: &boundflowv1.OperationUpdate{
				OperationId: opID,
				Result: &boundflowv1.AtomicOperationResult{
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
	mu         sync.Mutex
	inProgress []string
}

func newMockScheduler() *mockScheduler {
	return &mockScheduler{
		completeCh: make(chan string, 1),
		failCh:     make(chan string, 1),
	}
}

func (m *mockScheduler) CompleteRequest(_ context.Context, req string, _ string, _ int64, _ domain.CustomerRequestType, _ domain.RunOutcome, _ string, _ map[string]any) (bool, error) {
	m.completeCh <- req
	return true, nil
}

func (m *mockScheduler) FailRequest(_ context.Context, req string, _ string, _ int64, _ string, _ int) (bool, error) {
	m.failCh <- req
	return true, nil
}

func (m *mockScheduler) MarkInvoking(_ context.Context, _ string) error {
	return nil
}

func (m *mockScheduler) MarkRequestInProgress(_ context.Context, requestID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.inProgress = append(m.inProgress, requestID)
	return nil
}

func (m *mockScheduler) markedInProgress() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]string(nil), m.inProgress...)
}

func (m *mockScheduler) MarkAwaitingApproval(_ context.Context, _ string) error {
	return nil
}

func (m *mockScheduler) MarkAwaitingInput(_ context.Context, _ string) error {
	return nil
}

// ---- metrics handler mock ----

type mockMetrics struct{}

func (m *mockMetrics) MergeAgentMetrics(opMetrics map[string]*boundflowv1.AgentInvocationMetrics, jobMetrics *map[string]*boundflowv1.AgentInvocationMetrics) {
	if *jobMetrics == nil {
		*jobMetrics = map[string]*boundflowv1.AgentInvocationMetrics{}
	}
	for k, v := range opMetrics {
		(*jobMetrics)[k] = v
	}
}

func (m *mockMetrics) MergeWorkflowMetrics(opMetrics domain.WorkflowJobMetrics, jobMetrics *domain.WorkflowJobMetrics) {
	jobMetrics.Failures += opMetrics.Failures
}

// ---- constants and helpers ----

const (
	testWorkerID        = "test-worker"
	testWorkflowID      = "workflow-1"
	testRequestID       = "req-1"
	testTenantGroupID   = "test-group"
	testWorkflowType    = "test-workflow"
	testWorkflowVersion = int32(1)
)

func newTestWorker(ctrl *gomock.Controller) (*rpcworker.RpcWorker, *mocks.MockJobRepository, *mockScheduler) {
	jobRepo := mocks.NewMockJobRepository(ctrl)
	auditRepo := mocks.NewMockAuditRepository(ctrl)
	sched := newMockScheduler()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	return rpcworker.NewRpcWorker(jobRepo, auditRepo, testWorkerID, 60, sched, &mockMetrics{}, log), jobRepo, sched
}

func testJob() *domain.Job {
	return &domain.Job{
		WorkflowID:             testWorkflowID,
		RequestID:              testRequestID,
		CurrentAtomicOperation: "create",
		JobType:                "create",
		Status:                 domain.JobStatusPending,
		Context:                map[string]any{},
		RuntimeParams:          domain.WorkflowRuntimeParams{OperationTimeoutSeconds: 60},
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
	resID := testWorkflowID
	jobRepo.EXPECT().GetAvailableJob(gomock.Any(), testTenantGroupID, gomock.Any(), gomock.Any()).Return(&resID, nil)
	jobRepo.EXPECT().AcquireJob(gomock.Any(), testWorkflowID, gomock.Any(), gomock.Any(), testTenantGroupID).Return(testJob(), nil)
	jobRepo.EXPECT().DispatchJob(gomock.Any(), testWorkflowID, gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(true, nil).AnyTimes()
	jobRepo.EXPECT().RenewJobLease(gomock.Any(), testWorkflowID, gomock.Any(), gomock.Any()).Return(true, false, nil).AnyTimes()
	jobRepo.EXPECT().ReleaseJob(gomock.Any(), testWorkflowID, gomock.Any()).Return(nil).AnyTimes()
}

// driveToConnectedBusy sets up the UpdateJobStatus(running) expectation, pushes the
// initial ReadyForWork and IN_PROGRESS messages, reads the LaunchOperation from the
// stream, and blocks until the inner goroutine has confirmed running state (and is
// therefore in ConnectedBusy). WorkerSession must be running before calling this.
func driveToConnectedBusy(t *testing.T, jobRepo *mocks.MockJobRepository, stream *mockStream) {
	t.Helper()

	runningSet := make(chan struct{})
	jobRepo.EXPECT().
		UpdateJobStatus(gomock.Any(), testWorkflowID, gomock.Any(), domain.JobStatusRunning).
		DoAndReturn(func(_ context.Context, _, _ string, _ domain.JobStatus) (bool, error) {
			close(runningSet)
			return true, nil
		})

	stream.push(readyMsg())

	launch := <-stream.sendCh
	if launch.GetLaunch() == nil {
		t.Fatal("expected LaunchOperation from server")
	}

	stream.push(updateMsg(testRequestID, boundflowv1.OperationStatus_OPERATION_STATUS_IN_PROGRESS))
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

	stream.push(updateMsg(testRequestID, boundflowv1.OperationStatus_OPERATION_STATUS_IN_PROGRESS))

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
	jobRepo.EXPECT().GetAvailableJob(gomock.Any(), testTenantGroupID, gomock.Any(), gomock.Any()).
		DoAndReturn(func(context.Context, string, []string, []int32) (*string, error) {
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

	resID := testWorkflowID
	jobRepo.EXPECT().GetAvailableJob(gomock.Any(), testTenantGroupID, gomock.Any(), gomock.Any()).Return(&resID, nil)

	acquired := make(chan struct{})
	jobRepo.EXPECT().AcquireJob(gomock.Any(), testWorkflowID, gomock.Any(), gomock.Any(), testTenantGroupID).
		DoAndReturn(func(_ context.Context, _, _ string, _ time.Duration, _ string) (*domain.Job, error) {
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
	jobRepo.EXPECT().UpdateJobStatus(gomock.Any(), testWorkflowID, gomock.Any(), domain.JobStatusRunning).Return(true, nil)
	jobRepo.EXPECT().UpdateJobStatusWithMetrics(gomock.Any(), testWorkflowID, gomock.Any(), domain.JobStatusCompleted, gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(true, nil)

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

	stream.push(updateMsg(testRequestID, boundflowv1.OperationStatus_OPERATION_STATUS_IN_PROGRESS))
	stream.push(updateMsg(testRequestID, boundflowv1.OperationStatus_OPERATION_STATUS_COMPLETED))

	assertReceived(t, sched.completeCh, testRequestID, "CompleteRequest")

	cancel()
	if err := <-errCh; err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
}

// Dispatch must fence on the per-session owner: DispatchJob has to be called
// with the same owner AcquireJob received (the session id), never the worker id.
// Regression guard for the s.id-instead-of-sessionID owner bug, which made every
// dispatch return "lost ownership" so no operation ever launched.
func TestWorkerSession_Dispatch_UsesSessionOwner(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	worker, jobRepo, _ := newTestWorker(ctrl)

	resID := testWorkflowID
	jobRepo.EXPECT().GetAvailableJob(gomock.Any(), testTenantGroupID, gomock.Any(), gomock.Any()).Return(&resID, nil)

	var acquireOwner string
	jobRepo.EXPECT().AcquireJob(gomock.Any(), testWorkflowID, gomock.Any(), gomock.Any(), testTenantGroupID).
		DoAndReturn(func(_ context.Context, _, owner string, _ time.Duration, _ string) (*domain.Job, error) {
			acquireOwner = owner
			return testJob(), nil
		})

	dispatched := make(chan string, 1)
	jobRepo.EXPECT().DispatchJob(gomock.Any(), testWorkflowID, gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _, owner string, _ string, _ int, _ map[string]any,
			_ map[string]*boundflowv1.AgentInvocationMetrics, _ domain.WorkflowJobMetrics) (bool, error) {
			dispatched <- owner
			return true, nil
		})

	jobRepo.EXPECT().RenewJobLease(gomock.Any(), testWorkflowID, gomock.Any(), gomock.Any()).Return(true, false, nil).AnyTimes()
	jobRepo.EXPECT().ReleaseJob(gomock.Any(), testWorkflowID, gomock.Any()).Return(nil).AnyTimes()

	stream := newMockStream(ctx)
	errCh := runSession(worker, stream)

	stream.push(readyMsg())

	// A LaunchOperation only reaches the client if DispatchJob succeeded.
	launch := <-stream.sendCh
	if launch.GetLaunch() == nil {
		t.Fatal("expected LaunchOperation after dispatch")
	}

	select {
	case dispatchOwner := <-dispatched:
		if dispatchOwner == testWorkerID {
			t.Errorf("DispatchJob used the worker id %q as owner; must use the per-session owner", testWorkerID)
		}
		if dispatchOwner != acquireOwner {
			t.Errorf("dispatch owner %q != acquire owner %q; owner must be consistent within a session", dispatchOwner, acquireOwner)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("DispatchJob not called")
	}

	cancel()
	if err := <-errCh; err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
}

// When the LaunchOperation send fails the client never received it, so the worker
// must un-dispatch: reset the job to its pre-dispatch status (Pending) using the
// session owner and NOT fail the request (a transient send error is retryable).
func TestWorkerSession_LaunchSendFails_ResetsToPending(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	worker, jobRepo, sched := newTestWorker(ctrl)

	resID := testWorkflowID
	jobRepo.EXPECT().GetAvailableJob(gomock.Any(), testTenantGroupID, gomock.Any(), gomock.Any()).Return(&resID, nil).AnyTimes()

	var acquireOwner string
	jobRepo.EXPECT().AcquireJob(gomock.Any(), testWorkflowID, gomock.Any(), gomock.Any(), testTenantGroupID).
		DoAndReturn(func(_ context.Context, _, owner string, _ time.Duration, _ string) (*domain.Job, error) {
			acquireOwner = owner
			return testJob(), nil
		})
	jobRepo.EXPECT().DispatchJob(gomock.Any(), testWorkflowID, gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(true, nil)
	jobRepo.EXPECT().RenewJobLease(gomock.Any(), testWorkflowID, gomock.Any(), gomock.Any()).Return(true, false, nil).AnyTimes()
	jobRepo.EXPECT().ReleaseJob(gomock.Any(), testWorkflowID, gomock.Any()).Return(nil).AnyTimes()

	reset := make(chan string, 1)
	jobRepo.EXPECT().UpdateJobStatus(gomock.Any(), testWorkflowID, gomock.Any(), domain.JobStatusPending).
		DoAndReturn(func(_ context.Context, _, owner string, _ domain.JobStatus) (bool, error) {
			reset <- owner
			return true, nil
		})

	stream := newMockStream(ctx)
	stream.sendErr = errors.New("stream broken") // force the LaunchOperation send to fail
	errCh := runSession(worker, stream)

	stream.push(readyMsg())

	select {
	case owner := <-reset:
		if owner != acquireOwner {
			t.Errorf("reset owner %q != acquire owner %q", owner, acquireOwner)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("expected reset to Pending after Launch send failed")
	}

	// A transient send failure must not fail the request.
	select {
	case req := <-sched.failCh:
		t.Errorf("FailRequest called for %q; a failed Launch send should reset, not fail", req)
	case <-time.After(200 * time.Millisecond):
	}

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
	jobRepo.EXPECT().UpdateJobStatus(gomock.Any(), testWorkflowID, gomock.Any(), domain.JobStatusRunning).Return(true, nil)
	jobRepo.EXPECT().FailJobWithMetrics(gomock.Any(), testWorkflowID, gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(true, nil)

	stream := newMockStream(ctx)
	errCh := runSession(worker, stream)

	stream.push(readyMsg())
	<-stream.sendCh // LaunchOperation
	stream.push(updateMsg(testRequestID, boundflowv1.OperationStatus_OPERATION_STATUS_IN_PROGRESS))
	stream.push(updateMsg(testRequestID, boundflowv1.OperationStatus_OPERATION_STATUS_FAILED))

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
	jobRepo.EXPECT().FailJobWithMetrics(gomock.Any(), testWorkflowID, gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(true, nil)

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

	// Wrong op id in ConnectedWaiting fails the operation.
	jobRepo.EXPECT().FailJobWithMetrics(gomock.Any(), testWorkflowID, gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(true, nil)
	stream.push(updateMsg("wrong-op-id", boundflowv1.OperationStatus_OPERATION_STATUS_IN_PROGRESS))

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

	// Send COMPLETED before IN_PROGRESS — unexpected; fails the operation.
	jobRepo.EXPECT().FailJobWithMetrics(gomock.Any(), testWorkflowID, gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(true, nil)
	stream.push(updateMsg(testRequestID, boundflowv1.OperationStatus_OPERATION_STATUS_COMPLETED))

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
	jobRepo.EXPECT().FailJobWithMetrics(gomock.Any(), testWorkflowID, gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(true, nil)

	stream := newMockStream(ctx)
	errCh := runSession(worker, stream)

	driveToConnectedBusy(t, jobRepo, stream)

	stream.push(updateMsg("wrong-op-id", boundflowv1.OperationStatus_OPERATION_STATUS_COMPLETED))

	assertReceived(t, sched.failCh, testRequestID, "FailRequest")

	select {
	case <-errCh:
	case <-time.After(2 * time.Second):
		t.Fatal("WorkerSession did not return in time")
	}
}

// A job resumed from a gate carries its operation in job_metadata, not the live
// columns — parking emptied those. Without the dispatch write putting them back, a
// requeue mid-operation returns a job with no operation and a zero timeout.
func TestWorkerSession_Dispatch_PersistsAResolvedGateBranch(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	worker, jobRepo, _ := newTestWorker(ctrl)

	parked := testJob()
	parked.Status = domain.JobStatusApproved
	parked.CurrentAtomicOperation = ""
	parked.Context = map[string]any{}
	parked.RuntimeParams.OperationTimeoutSeconds = 0
	parked.JobMetadata = domain.JobMetadata{
		ApprovalGate: &domain.ApprovalGateMetadata{
			OnApprove: domain.ApprovalBranch{Next: &domain.NextOperation{
				OperationName:  "after_approval",
				Context:        map[string]any{"k": "v"},
				TimeoutSeconds: 1200,
			}},
		},
	}

	resID := testWorkflowID
	jobRepo.EXPECT().GetAvailableJob(gomock.Any(), testTenantGroupID, gomock.Any(), gomock.Any()).Return(&resID, nil)
	jobRepo.EXPECT().AcquireJob(gomock.Any(), testWorkflowID, gomock.Any(), gomock.Any(), testTenantGroupID).Return(parked, nil)
	jobRepo.EXPECT().RenewJobLease(gomock.Any(), testWorkflowID, gomock.Any(), gomock.Any()).Return(true, false, nil).AnyTimes()
	jobRepo.EXPECT().ReleaseJob(gomock.Any(), testWorkflowID, gomock.Any()).Return(nil).AnyTimes()

	type dispatch struct {
		operation string
		timeout   int
		context   map[string]any
	}
	got := make(chan dispatch, 1)
	jobRepo.EXPECT().DispatchJob(gomock.Any(), testWorkflowID, gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _, _ string, operation string, timeout int, jobContext map[string]any,
			_ map[string]*boundflowv1.AgentInvocationMetrics, _ domain.WorkflowJobMetrics) (bool, error) {
			got <- dispatch{operation, timeout, jobContext}
			return true, nil
		})

	stream := newMockStream(ctx)
	errCh := runSession(worker, stream)
	stream.push(readyMsg())

	select {
	case d := <-got:
		if d.operation != "after_approval" {
			t.Errorf("dispatched operation %q, want the resolved branch's operation", d.operation)
		}
		if d.timeout != 1200 {
			t.Errorf("dispatched timeout %d, want 1200; a zero here panics NewTicker", d.timeout)
		}
		if d.context["k"] != "v" {
			t.Errorf("dispatched context %v, want the branch's context", d.context)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("DispatchJob not called")
	}

	cancel()
	<-errCh
}

// A failed operation has spent as much as a successful one. Its final snapshot must
// reach the job row, or an interrupted run promotes numbers that stop at whatever the
// last interim report happened to catch — or at nothing, if none fired.
func TestWorkerSession_FailOperation_RecordsTheFinalSnapshot(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	worker, jobRepo, sched := newTestWorker(ctrl)
	expectJobAcquired(jobRepo)
	jobRepo.EXPECT().UpdateJobStatus(gomock.Any(), testWorkflowID, gomock.Any(), domain.JobStatusRunning).Return(true, nil)

	got := make(chan map[string]*boundflowv1.AgentInvocationMetrics, 1)
	jobRepo.EXPECT().FailJobWithMetrics(gomock.Any(), testWorkflowID, gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _, _, _ string,
			agentMetrics map[string]*boundflowv1.AgentInvocationMetrics, _ domain.WorkflowJobMetrics) (bool, error) {
			got <- agentMetrics
			return true, nil
		})

	stream := newMockStream(ctx)
	errCh := runSession(worker, stream)

	stream.push(readyMsg())
	<-stream.sendCh // LaunchOperation
	stream.push(updateMsg(testRequestID, boundflowv1.OperationStatus_OPERATION_STATUS_IN_PROGRESS))

	failed := updateMsg(testRequestID, boundflowv1.OperationStatus_OPERATION_STATUS_FAILED)
	cost := 0.75
	failed.GetUpdate().Result.AgentStateUpdates = map[string]*boundflowv1.AgentInvocationMetrics{
		"researcher": {CostUsd: &cost},
	}
	stream.push(failed)

	select {
	case m := <-got:
		if m["researcher"].GetCostUsd() != cost {
			t.Errorf("failed operation recorded cost %v, want %v", m["researcher"].GetCostUsd(), cost)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("FailJobWithMetrics not called")
	}

	assertReceived(t, sched.failCh, testRequestID, "FailRequest")
	cancel()
	<-errCh
}
