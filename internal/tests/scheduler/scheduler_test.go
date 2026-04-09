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
) {
	partitions := mocks.NewMockSchedulerPartitionRepository(ctrl)
	schedulerRepo := mocks.NewMockSchedulerRepository(ctrl)
	requests := mocks.NewMockCustomerRequestRepository(ctrl)
	resource := mocks.NewMockResourceInstanceRepository(ctrl)
	s := scheduler.New("test", 30, partitions, schedulerRepo, requests, resource, discardLogger)
	return s, partitions, schedulerRepo, requests, resource
}

// --- ScheduleRequest ---

func TestScheduleRequest_WrittenSupercedes(t *testing.T) {
	ctrl := gomock.NewController(t)
	s, _, schedulerRepo, _, _ := newTestScheduler(ctrl)

	schedulerRepo.EXPECT().
		UpsertJobAndSchedule(gomock.Any(), "req-1").
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
	s, _, schedulerRepo, _, _ := newTestScheduler(ctrl)

	schedulerRepo.EXPECT().
		UpsertJobAndSchedule(gomock.Any(), "req-1").
		Return("", int64(0), false, nil)

	// SupercedeOlderRequests must NOT be called
	if err := s.ScheduleRequest(context.Background(), "req-1"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestScheduleRequest_UpsertError(t *testing.T) {
	ctrl := gomock.NewController(t)
	s, _, schedulerRepo, _, _ := newTestScheduler(ctrl)

	schedulerRepo.EXPECT().
		UpsertJobAndSchedule(gomock.Any(), "req-1").
		Return("", int64(0), false, errors.New("db error"))

	if err := s.ScheduleRequest(context.Background(), "req-1"); err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestScheduleRequest_SupercedeError(t *testing.T) {
	ctrl := gomock.NewController(t)
	s, _, schedulerRepo, _, _ := newTestScheduler(ctrl)

	schedulerRepo.EXPECT().
		UpsertJobAndSchedule(gomock.Any(), "req-1").
		Return("resource-1", int64(3), true, nil)

	schedulerRepo.EXPECT().
		SupercedeOlderRequests(gomock.Any(), "resource-1", int64(3)).
		Return(errors.New("db error"))

	if err := s.ScheduleRequest(context.Background(), "req-1"); err == nil {
		t.Fatal("expected error, got nil")
	}
}

// --- CompleteRequest ---

func TestCompleteRequest_Create_TransitionsToActive(t *testing.T) {
	ctrl := gomock.NewController(t)
	s, _, schedulerRepo, requests, resource := newTestScheduler(ctrl)

	goalState := domain.ResourceState{"sku": "standard"}
	requests.EXPECT().
		CompleteRequest(gomock.Any(), "req-1").
		Return(&domain.CustomerRequest{
			ID:                 "req-1",
			ResourceInstanceID: "resource-1",
			RequestType:        domain.CustomerRequestTypeCreate,
			GoalConfigSnapshot: goalState,
			Version:            1,
		}, nil)

	resource.EXPECT().
		ApplyCompletedJob(gomock.Any(), "resource-1", goalState, domain.LifecycleStateActive, int64(1)).
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
	s, _, schedulerRepo, requests, resource := newTestScheduler(ctrl)

	requests.EXPECT().
		CompleteRequest(gomock.Any(), "req-1").
		Return(&domain.CustomerRequest{
			ID:                 "req-1",
			ResourceInstanceID: "resource-1",
			RequestType:        domain.CustomerRequestTypeDelete,
			GoalConfigSnapshot: domain.ResourceState{},
			Version:            2,
		}, nil)

	resource.EXPECT().
		ApplyCompletedJob(gomock.Any(), "resource-1", domain.ResourceState{}, domain.LifecycleStateDeleted, int64(2)).
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
	s, _, schedulerRepo, requests, resource := newTestScheduler(ctrl)

	goalState := domain.ResourceState{"sku": "standard"}
	requests.EXPECT().
		CompleteRequest(gomock.Any(), "req-1").
		Return(&domain.CustomerRequest{
			ID:                 "req-1",
			ResourceInstanceID: "resource-1",
			RequestType:        domain.CustomerRequestTypeReconcile,
			GoalConfigSnapshot: goalState,
			Version:            1,
		}, nil)

	resource.EXPECT().
		ApplyCompletedJob(gomock.Any(), "resource-1", goalState, domain.LifecycleStateActive, int64(1)).
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
	s, _, _, requests, _ := newTestScheduler(ctrl)

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
	s, _, schedulerRepo, requests, resource := newTestScheduler(ctrl)

	goalState := domain.ResourceState{"sku": "standard"}
	requests.EXPECT().
		FailRequest(gomock.Any(), "req-1").
		Return(&domain.CustomerRequest{
			ID:                 "req-1",
			ResourceInstanceID: "resource-1",
			GoalConfigSnapshot: goalState,
			Version:            2,
		}, nil)

	resource.EXPECT().
		ApplyCompletedJob(gomock.Any(), "resource-1", goalState, domain.LifecycleStateFailed, int64(2)).
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
	s, _, schedulerRepo, requests, resource := newTestScheduler(ctrl)

	goalState := domain.ResourceState{}
	requests.EXPECT().
		FailRequest(gomock.Any(), "req-1").
		Return(&domain.CustomerRequest{
			ID:                 "req-1",
			ResourceInstanceID: "resource-1",
			GoalConfigSnapshot: goalState,
			Version:            1,
		}, nil)

	resource.EXPECT().
		ApplyCompletedJob(gomock.Any(), "resource-1", goalState, domain.LifecycleStateFailed, int64(1)).
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
	s, _, _, requests, _ := newTestScheduler(ctrl)

	requests.EXPECT().
		FailRequest(gomock.Any(), "req-1").
		Return(nil, errors.New("db error"))

	if _, err := s.FailRequest(context.Background(), "req-1"); err == nil {
		t.Fatal("expected error, got nil")
	}
}
