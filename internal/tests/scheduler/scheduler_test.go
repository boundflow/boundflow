package scheduler_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"

	"go.uber.org/mock/gomock"

	boundflowv1 "github.com/boundflow/boundflow/gen/boundflow/v1"
	"github.com/boundflow/boundflow/internal/domain"
	"github.com/boundflow/boundflow/internal/scheduler"
	"github.com/boundflow/boundflow/internal/storage/mocks"
)

var discardLogger = slog.New(slog.NewTextHandler(io.Discard, nil))

func i32(v int32) *int32 { return &v }

// no-op doubles for the post-completion metric + policy steps in CompleteRequest.
type noopMetricsHandler struct{}

func (noopMetricsHandler) HandleAgentMetrics(_ context.Context, _ map[string]*boundflowv1.AgentInvocationMetrics, _ domain.WorkflowJobMetrics, _ *domain.Workflow) (error, *domain.WorkflowVersionMetrics) {
	return nil, nil
}

type noopPolicyResolver struct{}

func (noopPolicyResolver) ResolveLifecyclePolicy(_ context.Context, _ *domain.Workflow, _ *domain.WorkflowVersionMetrics) (*domain.PolicyActionDetails, error) {
	return nil, nil
}

func newTestScheduler(ctrl *gomock.Controller) (
	*scheduler.Scheduler,
	*mocks.MockSchedulerPartitionRepository,
	*mocks.MockSchedulerRepository,
	*mocks.MockCustomerRequestRepository,
	*mocks.MockWorkflowRepository,
	*mocks.MockAgentStateRepository,
) {
	partitions := mocks.NewMockSchedulerPartitionRepository(ctrl)
	schedulerRepo := mocks.NewMockSchedulerRepository(ctrl)
	requests := mocks.NewMockCustomerRequestRepository(ctrl)
	workflow := mocks.NewMockWorkflowRepository(ctrl)
	agentStates := mocks.NewMockAgentStateRepository(ctrl)
	jobs := mocks.NewMockJobRepository(ctrl)
	// CompleteRequest pulls agent metrics off the job; default to none so tests that don't
	// care about metrics don't need to set it up. The no-op metrics/resolver doubles below
	// keep the post-completion metric+policy steps inert.
	jobs.EXPECT().GetJobMetrics(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, domain.WorkflowJobMetrics{}, nil).AnyTimes()
	// Default workflow passes validateWorkflowState (active + metrics resolved up to current run).
	workflow.EXPECT().Get(gomock.Any(), gomock.Any()).Return(&domain.Workflow{
		ID:                     "workflow-1",
		CurrentWorkflowVersion: 1,
		CurrentVersion:         1,
		LifecycleLastResolved:  1,
		Lifecycle:              domain.LifecycleInfo{State: domain.LifecycleStateActive},
		WorkflowState:          domain.WorkflowStateActive,
	}, nil).AnyTimes()
	s := scheduler.NewScheduler("test", 30, 25, partitions, schedulerRepo, requests, workflow, agentStates, jobs, noopMetricsHandler{}, noopPolicyResolver{}, mocks.NewMockAuditRepository(ctrl), discardLogger)
	return s, partitions, schedulerRepo, requests, workflow, agentStates
}

// --- ScheduleRequest ---

// testCustomerRequest is a minimal CustomerRequest used across ScheduleRequest tests.
var testCustomerRequest = &domain.CustomerRequest{
	ID:                 "req-1",
	WorkflowID: "workflow-1",
	RequestType:        domain.CustomerRequestTypeInvoke,
	RequestInfo:        map[string]any{"correlationId": "corr-1", "operationTimeoutSeconds": float64(30), "initialVersion": float64(1)},
}

func TestScheduleRequest_WrittenSupercedes(t *testing.T) {
	ctrl := gomock.NewController(t)
	s, _, schedulerRepo, requests, _, agentStates := newTestScheduler(ctrl)

	requests.EXPECT().Get(gomock.Any(), "req-1").Return(testCustomerRequest, nil)
	agentStates.EXPECT().GetAllForWorkflow(gomock.Any(), "workflow-1").Return(nil, nil)

	schedulerRepo.EXPECT().
		UpsertJobAndSchedule(gomock.Any(), "req-1", gomock.Any(), "invoke_entry", 30, 1, int64(1), gomock.Any()).
		Return("workflow-1", int64(3), true, nil)

	schedulerRepo.EXPECT().
		SupercedeOlderRequests(gomock.Any(), "workflow-1", int64(3)).
		Return(nil)

	schedulerRepo.EXPECT().
		MarkWorkflowScheduled(gomock.Any(), "workflow-1").
		Return(nil)

	if err := s.ScheduleRequest(context.Background(), "req-1"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestScheduleRequest_NotWritten_NoSupercede(t *testing.T) {
	ctrl := gomock.NewController(t)
	s, _, schedulerRepo, requests, _, agentStates := newTestScheduler(ctrl)

	requests.EXPECT().Get(gomock.Any(), "req-1").Return(testCustomerRequest, nil)
	agentStates.EXPECT().GetAllForWorkflow(gomock.Any(), "workflow-1").Return(nil, nil)

	schedulerRepo.EXPECT().
		UpsertJobAndSchedule(gomock.Any(), "req-1", gomock.Any(), "invoke_entry", 30, 1, int64(1), gomock.Any()).
		Return("", int64(0), false, nil)

	// SupercedeOlderRequests must NOT be called
	if err := s.ScheduleRequest(context.Background(), "req-1"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestScheduleRequest_UpsertError(t *testing.T) {
	ctrl := gomock.NewController(t)
	s, _, schedulerRepo, requests, _, agentStates := newTestScheduler(ctrl)

	requests.EXPECT().Get(gomock.Any(), "req-1").Return(testCustomerRequest, nil)
	agentStates.EXPECT().GetAllForWorkflow(gomock.Any(), "workflow-1").Return(nil, nil)

	schedulerRepo.EXPECT().
		UpsertJobAndSchedule(gomock.Any(), "req-1", gomock.Any(), "invoke_entry", 30, 1, int64(1), gomock.Any()).
		Return("", int64(0), false, errors.New("db error"))

	if err := s.ScheduleRequest(context.Background(), "req-1"); err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestScheduleRequest_SupercedeError(t *testing.T) {
	ctrl := gomock.NewController(t)
	s, _, schedulerRepo, requests, _, agentStates := newTestScheduler(ctrl)

	requests.EXPECT().Get(gomock.Any(), "req-1").Return(testCustomerRequest, nil)
	agentStates.EXPECT().GetAllForWorkflow(gomock.Any(), "workflow-1").Return(nil, nil)

	schedulerRepo.EXPECT().
		UpsertJobAndSchedule(gomock.Any(), "req-1", gomock.Any(), "invoke_entry", 30, 1, int64(1), gomock.Any()).
		Return("workflow-1", int64(3), true, nil)

	schedulerRepo.EXPECT().
		SupercedeOlderRequests(gomock.Any(), "workflow-1", int64(3)).
		Return(errors.New("db error"))

	if err := s.ScheduleRequest(context.Background(), "req-1"); err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestScheduleRequest_GetRequestError(t *testing.T) {
	ctrl := gomock.NewController(t)
	s, _, _, requests, _, _ := newTestScheduler(ctrl)

	requests.EXPECT().Get(gomock.Any(), "req-1").Return(nil, errors.New("db error"))

	if err := s.ScheduleRequest(context.Background(), "req-1"); err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestScheduleRequest_GetAgentStatesError(t *testing.T) {
	ctrl := gomock.NewController(t)
	s, _, _, requests, _, agentStates := newTestScheduler(ctrl)

	requests.EXPECT().Get(gomock.Any(), "req-1").Return(testCustomerRequest, nil)
	agentStates.EXPECT().GetAllForWorkflow(gomock.Any(), "workflow-1").Return(nil, errors.New("db error"))

	if err := s.ScheduleRequest(context.Background(), "req-1"); err == nil {
		t.Fatal("expected error, got nil")
	}
}

// TestScheduleRequest_AgentStateInContext verifies that live agent lifecycle policy and metrics
// are merged into the job context, and the entry operation name is derived from request type.
func TestScheduleRequest_AgentStateInContext(t *testing.T) {
	ctrl := gomock.NewController(t)
	s, _, schedulerRepo, requests, _, agentStates := newTestScheduler(ctrl)

	requests.EXPECT().Get(gomock.Any(), "req-1").Return(testCustomerRequest, nil)
	agentStates.EXPECT().GetAllForWorkflow(gomock.Any(), "workflow-1").Return(map[string]*domain.AgentState{
		"my_agent": {
			AgentName:         "my_agent",
			LifecyclePolicy:   map[string]any{"rules": []any{}},
			InvocationMetrics: []*boundflowv1.AgentInvocationMetrics{{}},
		},
	}, nil)

	schedulerRepo.EXPECT().
		UpsertJobAndSchedule(gomock.Any(), "req-1", gomock.AssignableToTypeOf(""), "invoke_entry", 30, 1, int64(1), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ string, contextJSON string, op string, timeout int, wfVersion int, expectedCurrentVersion int64, _ string) (string, int64, bool, error) {
			if contextJSON == "" || contextJSON == "{}" {
				t.Error("expected non-empty context JSON")
			}
			if op != "invoke_entry" {
				t.Errorf("expected op invoke_entry, got %s", op)
			}
			if timeout != 30 {
				t.Errorf("expected timeout 30, got %d", timeout)
			}
			return "workflow-1", int64(1), true, nil
		})

	schedulerRepo.EXPECT().
		SupercedeOlderRequests(gomock.Any(), "workflow-1", int64(1)).
		Return(nil)

	schedulerRepo.EXPECT().
		MarkWorkflowScheduled(gomock.Any(), "workflow-1").
		Return(nil)

	if err := s.ScheduleRequest(context.Background(), "req-1"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

// --- UpdateAgentMetrics ---

func TestUpdateAgentMetrics_CallsRepoForEachAgent(t *testing.T) {
	ctrl := gomock.NewController(t)
	s, _, _, _, _, agentStates := newTestScheduler(ctrl)

	metrics1 := []*boundflowv1.AgentInvocationMetrics{{TokensUsed: i32(100)}}
	metrics2 := []*boundflowv1.AgentInvocationMetrics{{TokensUsed: i32(200)}}

	agentStates.EXPECT().
		UpdateMetrics(gomock.Any(), "workflow-1", "agent_a", metrics1).
		Return(nil)
	agentStates.EXPECT().
		UpdateMetrics(gomock.Any(), "workflow-1", "agent_b", metrics2).
		Return(nil)

	err := s.UpdateAgentMetrics(context.Background(), "workflow-1", map[string][]*boundflowv1.AgentInvocationMetrics{
		"agent_a": metrics1,
		"agent_b": metrics2,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestUpdateAgentMetrics_RepoErrorDoesNotStop(t *testing.T) {
	ctrl := gomock.NewController(t)
	s, _, _, _, _, agentStates := newTestScheduler(ctrl)

	agentStates.EXPECT().
		UpdateMetrics(gomock.Any(), "workflow-1", "agent_a", gomock.Any()).
		Return(errors.New("db error"))

	// UpdateAgentMetrics logs and continues — should return nil.
	err := s.UpdateAgentMetrics(context.Background(), "workflow-1", map[string][]*boundflowv1.AgentInvocationMetrics{
		"agent_a": {{TokensUsed: i32(50)}},
	})
	if err != nil {
		t.Fatalf("expected nil despite repo error, got %v", err)
	}
}

// --- CompleteRequest ---

func TestCompleteRequest_Create_TransitionsToActive(t *testing.T) {
	ctrl := gomock.NewController(t)
	s, _, schedulerRepo, requests, workflow, _ := newTestScheduler(ctrl)

	requests.EXPECT().
		CompleteRequest(gomock.Any(), "req-1", gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&domain.CustomerRequest{
			ID:                 "req-1",
			WorkflowID: "workflow-1",
			RequestType:        domain.CustomerRequestTypeCreate,
						Version:            1,
		}, nil)

	workflow.EXPECT().
		ApplyCompletedJob(gomock.Any(), "workflow-1", domain.LifecycleStateActive, int64(1)).
		Return(true, nil)

	schedulerRepo.EXPECT().
		DeleteTerminalJob(gomock.Any(), "workflow-1", "req-1").
		Return(true, nil)

	applied, err := s.CompleteRequest(context.Background(), "req-1", domain.RunOutcomeSuccessful, "", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !applied {
		t.Error("expected applied=true")
	}
}

func TestCompleteRequest_Delete_TransitionsToDeleted(t *testing.T) {
	ctrl := gomock.NewController(t)
	s, _, schedulerRepo, requests, workflow, _ := newTestScheduler(ctrl)

	requests.EXPECT().
		CompleteRequest(gomock.Any(), "req-1", gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&domain.CustomerRequest{
			ID:                 "req-1",
			WorkflowID: "workflow-1",
			RequestType:        domain.CustomerRequestTypeDelete,
			Version:            2,
		}, nil)

	workflow.EXPECT().
		ApplyCompletedJob(gomock.Any(), "workflow-1", domain.LifecycleStateDeleted, int64(2)).
		Return(true, nil)

	schedulerRepo.EXPECT().
		DeleteTerminalJob(gomock.Any(), "workflow-1", "req-1").
		Return(true, nil)

	applied, err := s.CompleteRequest(context.Background(), "req-1", domain.RunOutcomeSuccessful, "", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !applied {
		t.Error("expected applied=true")
	}
}

func TestCompleteRequest_VersionSkipped_ReturnsFalse(t *testing.T) {
	ctrl := gomock.NewController(t)
	s, _, schedulerRepo, requests, workflow, _ := newTestScheduler(ctrl)

	requests.EXPECT().
		CompleteRequest(gomock.Any(), "req-1", gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&domain.CustomerRequest{
			ID:                 "req-1",
			WorkflowID: "workflow-1",
			RequestType:        domain.CustomerRequestTypeInvoke,
						Version:            1,
		}, nil)

	workflow.EXPECT().
		ApplyCompletedJob(gomock.Any(), "workflow-1", domain.LifecycleStateActive, int64(1)).
		Return(false, nil)

	schedulerRepo.EXPECT().
		DeleteTerminalJob(gomock.Any(), "workflow-1", "req-1").
		Return(true, nil)

	applied, err := s.CompleteRequest(context.Background(), "req-1", domain.RunOutcomeSuccessful, "", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if applied {
		t.Error("expected applied=false when version check fails")
	}
}

func TestCompleteRequest_FailRequestError(t *testing.T) {
	ctrl := gomock.NewController(t)
	s, _, _, requests, _, _ := newTestScheduler(ctrl)

	requests.EXPECT().
		CompleteRequest(gomock.Any(), "req-1", gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil, errors.New("db error"))

	if _, err := s.CompleteRequest(context.Background(), "req-1", domain.RunOutcomeSuccessful, "", nil); err == nil {
		t.Fatal("expected error, got nil")
	}
}

// --- FailRequest ---

func TestFailRequest_AppliesFailedState(t *testing.T) {
	ctrl := gomock.NewController(t)
	s, _, schedulerRepo, requests, workflow, _ := newTestScheduler(ctrl)

	requests.EXPECT().
		FailRequest(gomock.Any(), "req-1", gomock.Any()).
		Return(&domain.CustomerRequest{
			ID:                 "req-1",
			WorkflowID: "workflow-1",
						Version:            2,
		}, nil)

	workflow.EXPECT().
		ApplyFailedJob(gomock.Any(), "workflow-1", "req-1", domain.LifecycleStateInterrupted, domain.WorkflowStateDisabled, int64(2)).
		Return(true, nil)

	schedulerRepo.EXPECT().
		DeleteTerminalJob(gomock.Any(), "workflow-1", "req-1").
		Return(true, nil)

	applied, err := s.FailRequest(context.Background(), "req-1", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !applied {
		t.Error("expected applied=true")
	}
}

func TestFailRequest_VersionSkipped_ReturnsFalse(t *testing.T) {
	ctrl := gomock.NewController(t)
	s, _, schedulerRepo, requests, workflow, _ := newTestScheduler(ctrl)

	requests.EXPECT().
		FailRequest(gomock.Any(), "req-1", gomock.Any()).
		Return(&domain.CustomerRequest{
			ID:                 "req-1",
			WorkflowID: "workflow-1",
						Version:            1,
		}, nil)

	workflow.EXPECT().
		ApplyFailedJob(gomock.Any(), "workflow-1", "req-1", domain.LifecycleStateInterrupted, domain.WorkflowStateDisabled, int64(1)).
		Return(false, nil)

	schedulerRepo.EXPECT().
		DeleteTerminalJob(gomock.Any(), "workflow-1", "req-1").
		Return(true, nil)

	applied, err := s.FailRequest(context.Background(), "req-1", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if applied {
		t.Error("expected applied=false when version check fails")
	}
}

func TestFailRequest_RepoError(t *testing.T) {
	ctrl := gomock.NewController(t)
	s, _, _, requests, _, _ := newTestScheduler(ctrl)

	requests.EXPECT().
		FailRequest(gomock.Any(), "req-1", gomock.Any()).
		Return(nil, errors.New("db error"))

	if _, err := s.FailRequest(context.Background(), "req-1", ""); err == nil {
		t.Fatal("expected error, got nil")
	}
}
