package scheduler_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"

	"go.uber.org/mock/gomock"

	"github.com/convergeplane/convergeplane/internal/domain"
	"github.com/convergeplane/convergeplane/internal/scheduler"
	"github.com/convergeplane/convergeplane/internal/storage/mocks"
)

var discardLogger = slog.New(slog.NewTextHandler(io.Discard, nil))

func newTestScheduler(ctrl *gomock.Controller) (
	*scheduler.Scheduler,
	*mocks.MockSchedulerPartitionRepository,
	*mocks.MockSchedulerRepository,
	*mocks.MockCustomerRequestRepository,
	*mocks.MockResourceInstanceRepository,
	*mocks.MockAgentStateRepository,
) {
	partitions := mocks.NewMockSchedulerPartitionRepository(ctrl)
	schedulerRepo := mocks.NewMockSchedulerRepository(ctrl)
	requests := mocks.NewMockCustomerRequestRepository(ctrl)
	resource := mocks.NewMockResourceInstanceRepository(ctrl)
	agentStates := mocks.NewMockAgentStateRepository(ctrl)
	s := scheduler.New("test", 30, partitions, schedulerRepo, requests, resource, agentStates, discardLogger)
	return s, partitions, schedulerRepo, requests, resource, agentStates
}

// --- ScheduleRequest ---

// testCustomerRequest is a minimal CustomerRequest used across ScheduleRequest tests.
var testCustomerRequest = &domain.CustomerRequest{
	ID:                 "req-1",
	ResourceInstanceID: "resource-1",
	RequestType:        domain.CustomerRequestTypeReconcile,
	RequestInfo:        map[string]any{"correlationId": "corr-1", "operationTimeoutSeconds": 30},
}

func TestScheduleRequest_WrittenSupercedes(t *testing.T) {
	ctrl := gomock.NewController(t)
	s, _, schedulerRepo, requests, _, agentStates := newTestScheduler(ctrl)

	requests.EXPECT().Get(gomock.Any(), "req-1").Return(testCustomerRequest, nil)
	agentStates.EXPECT().GetAllForResource(gomock.Any(), "resource-1").Return(nil, nil)

	schedulerRepo.EXPECT().
		UpsertJobAndSchedule(gomock.Any(), "req-1", gomock.Any(), "reconcile_entry").
		Return("resource-1", int64(3), true, nil)

	schedulerRepo.EXPECT().
		SupercedeOlderRequests(gomock.Any(), "resource-1", int64(3)).
		Return(nil)

	if err := s.ScheduleRequest(context.Background(), "req-1"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestScheduleRequest_NotWritten_NoSupercede(t *testing.T) {
	ctrl := gomock.NewController(t)
	s, _, schedulerRepo, requests, _, agentStates := newTestScheduler(ctrl)

	requests.EXPECT().Get(gomock.Any(), "req-1").Return(testCustomerRequest, nil)
	agentStates.EXPECT().GetAllForResource(gomock.Any(), "resource-1").Return(nil, nil)

	schedulerRepo.EXPECT().
		UpsertJobAndSchedule(gomock.Any(), "req-1", gomock.Any(), "reconcile_entry").
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
	agentStates.EXPECT().GetAllForResource(gomock.Any(), "resource-1").Return(nil, nil)

	schedulerRepo.EXPECT().
		UpsertJobAndSchedule(gomock.Any(), "req-1", gomock.Any(), "reconcile_entry").
		Return("", int64(0), false, errors.New("db error"))

	if err := s.ScheduleRequest(context.Background(), "req-1"); err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestScheduleRequest_SupercedeError(t *testing.T) {
	ctrl := gomock.NewController(t)
	s, _, schedulerRepo, requests, _, agentStates := newTestScheduler(ctrl)

	requests.EXPECT().Get(gomock.Any(), "req-1").Return(testCustomerRequest, nil)
	agentStates.EXPECT().GetAllForResource(gomock.Any(), "resource-1").Return(nil, nil)

	schedulerRepo.EXPECT().
		UpsertJobAndSchedule(gomock.Any(), "req-1", gomock.Any(), "reconcile_entry").
		Return("resource-1", int64(3), true, nil)

	schedulerRepo.EXPECT().
		SupercedeOlderRequests(gomock.Any(), "resource-1", int64(3)).
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
	agentStates.EXPECT().GetAllForResource(gomock.Any(), "resource-1").Return(nil, errors.New("db error"))

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
	agentStates.EXPECT().GetAllForResource(gomock.Any(), "resource-1").Return([]*domain.AgentState{
		{
			AgentName:         "my_agent",
			LifecyclePolicy:   map[string]any{"rules": []any{}},
			InvocationMetrics: []map[string]any{{"tokens": float64(100)}},
		},
	}, nil)

	schedulerRepo.EXPECT().
		UpsertJobAndSchedule(gomock.Any(), "req-1", gomock.AssignableToTypeOf(""), "reconcile_entry").
		DoAndReturn(func(_ context.Context, _ string, contextJSON string, op string) (string, int64, bool, error) {
			if contextJSON == "" || contextJSON == "{}" {
				t.Error("expected non-empty context JSON")
			}
			if op != "reconcile_entry" {
				t.Errorf("expected op reconcile_entry, got %s", op)
			}
			return "resource-1", int64(1), true, nil
		})

	schedulerRepo.EXPECT().
		SupercedeOlderRequests(gomock.Any(), "resource-1", int64(1)).
		Return(nil)

	if err := s.ScheduleRequest(context.Background(), "req-1"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

// --- UpdateAgentMetrics ---

func TestUpdateAgentMetrics_CallsRepoForEachAgent(t *testing.T) {
	ctrl := gomock.NewController(t)
	s, _, _, _, _, agentStates := newTestScheduler(ctrl)

	metrics1 := []map[string]any{{"tokens": 100}}
	metrics2 := []map[string]any{{"tokens": 200}}

	agentStates.EXPECT().
		UpdateMetrics(gomock.Any(), "resource-1", "agent_a", metrics1).
		Return(nil)
	agentStates.EXPECT().
		UpdateMetrics(gomock.Any(), "resource-1", "agent_b", metrics2).
		Return(nil)

	err := s.UpdateAgentMetrics(context.Background(), "resource-1", map[string][]map[string]any{
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
		UpdateMetrics(gomock.Any(), "resource-1", "agent_a", gomock.Any()).
		Return(errors.New("db error"))

	// UpdateAgentMetrics logs and continues — should return nil.
	err := s.UpdateAgentMetrics(context.Background(), "resource-1", map[string][]map[string]any{
		"agent_a": {{"tokens": 50}},
	})
	if err != nil {
		t.Fatalf("expected nil despite repo error, got %v", err)
	}
}

// --- CompleteRequest ---

func TestCompleteRequest_Create_TransitionsToActive(t *testing.T) {
	ctrl := gomock.NewController(t)
	s, _, schedulerRepo, requests, resource, _ := newTestScheduler(ctrl)

	requests.EXPECT().
		CompleteRequest(gomock.Any(), "req-1").
		Return(&domain.CustomerRequest{
			ID:                 "req-1",
			ResourceInstanceID: "resource-1",
			RequestType:        domain.CustomerRequestTypeCreate,
						Version:            1,
		}, nil)

	resource.EXPECT().
		ApplyCompletedJob(gomock.Any(), "resource-1", domain.LifecycleStateActive, int64(1)).
		Return(true, nil)

	schedulerRepo.EXPECT().
		DeleteTerminalJob(gomock.Any(), "resource-1", "req-1").
		Return(true, nil)

	applied, err := s.CompleteRequest(context.Background(), "req-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !applied {
		t.Error("expected applied=true")
	}
}

func TestCompleteRequest_Delete_TransitionsToDeleted(t *testing.T) {
	ctrl := gomock.NewController(t)
	s, _, schedulerRepo, requests, resource, _ := newTestScheduler(ctrl)

	requests.EXPECT().
		CompleteRequest(gomock.Any(), "req-1").
		Return(&domain.CustomerRequest{
			ID:                 "req-1",
			ResourceInstanceID: "resource-1",
			RequestType:        domain.CustomerRequestTypeDelete,
			Version:            2,
		}, nil)

	resource.EXPECT().
		ApplyCompletedJob(gomock.Any(), "resource-1", domain.LifecycleStateDeleted, int64(2)).
		Return(true, nil)

	schedulerRepo.EXPECT().
		DeleteTerminalJob(gomock.Any(), "resource-1", "req-1").
		Return(true, nil)

	applied, err := s.CompleteRequest(context.Background(), "req-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !applied {
		t.Error("expected applied=true")
	}
}

func TestCompleteRequest_VersionSkipped_ReturnsFalse(t *testing.T) {
	ctrl := gomock.NewController(t)
	s, _, schedulerRepo, requests, resource, _ := newTestScheduler(ctrl)

	requests.EXPECT().
		CompleteRequest(gomock.Any(), "req-1").
		Return(&domain.CustomerRequest{
			ID:                 "req-1",
			ResourceInstanceID: "resource-1",
			RequestType:        domain.CustomerRequestTypeReconcile,
						Version:            1,
		}, nil)

	resource.EXPECT().
		ApplyCompletedJob(gomock.Any(), "resource-1", domain.LifecycleStateActive, int64(1)).
		Return(false, nil)

	schedulerRepo.EXPECT().
		DeleteTerminalJob(gomock.Any(), "resource-1", "req-1").
		Return(true, nil)

	applied, err := s.CompleteRequest(context.Background(), "req-1")
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
		CompleteRequest(gomock.Any(), "req-1").
		Return(nil, errors.New("db error"))

	if _, err := s.CompleteRequest(context.Background(), "req-1"); err == nil {
		t.Fatal("expected error, got nil")
	}
}

// --- FailRequest ---

func TestFailRequest_AppliesFailedState(t *testing.T) {
	ctrl := gomock.NewController(t)
	s, _, schedulerRepo, requests, resource, _ := newTestScheduler(ctrl)

	requests.EXPECT().
		FailRequest(gomock.Any(), "req-1").
		Return(&domain.CustomerRequest{
			ID:                 "req-1",
			ResourceInstanceID: "resource-1",
						Version:            2,
		}, nil)

	resource.EXPECT().
		ApplyCompletedJob(gomock.Any(), "resource-1", domain.LifecycleStateFailed, int64(2)).
		Return(true, nil)

	schedulerRepo.EXPECT().
		DeleteTerminalJob(gomock.Any(), "resource-1", "req-1").
		Return(true, nil)

	applied, err := s.FailRequest(context.Background(), "req-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !applied {
		t.Error("expected applied=true")
	}
}

func TestFailRequest_VersionSkipped_ReturnsFalse(t *testing.T) {
	ctrl := gomock.NewController(t)
	s, _, schedulerRepo, requests, resource, _ := newTestScheduler(ctrl)

	requests.EXPECT().
		FailRequest(gomock.Any(), "req-1").
		Return(&domain.CustomerRequest{
			ID:                 "req-1",
			ResourceInstanceID: "resource-1",
						Version:            1,
		}, nil)

	resource.EXPECT().
		ApplyCompletedJob(gomock.Any(), "resource-1", domain.LifecycleStateFailed, int64(1)).
		Return(false, nil)

	schedulerRepo.EXPECT().
		DeleteTerminalJob(gomock.Any(), "resource-1", "req-1").
		Return(true, nil)

	applied, err := s.FailRequest(context.Background(), "req-1")
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
		FailRequest(gomock.Any(), "req-1").
		Return(nil, errors.New("db error"))

	if _, err := s.FailRequest(context.Background(), "req-1"); err == nil {
		t.Fatal("expected error, got nil")
	}
}
