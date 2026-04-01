package service_test

import (
	"context"
	"testing"

	"go.uber.org/mock/gomock"

	"github.com/convergeplane/convergeplane/internal/domain"
	"github.com/convergeplane/convergeplane/internal/service"
	"github.com/convergeplane/convergeplane/internal/storage/mocks"
)

func TestCreateResource(t *testing.T) {
	ctrl := gomock.NewController(t)
	resourceInstanceRepo := mocks.NewMockResourceInstanceRepository(ctrl)
	customerRequestRepo := mocks.NewMockCustomerRequestRepository(ctrl)
	schedulerRepo := mocks.NewMockSchedulerRepository(ctrl)
	svc := service.NewLifecycleService(resourceInstanceRepo, customerRequestRepo, schedulerRepo)

	initialState := domain.ResourceState{"sku": "standard"}

	resourceInstanceRepo.EXPECT().
		Create(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, r *domain.ResourceInstance) error {
			if r.ID == "" {
				t.Error("expected ID to be generated")
			}
			if r.TenantID != "tenant-1" {
				t.Errorf("expected tenant_id tenant-1, got %s", r.TenantID)
			}
			if r.ResourceType != "database" {
				t.Errorf("expected resource_type database, got %s", r.ResourceType)
			}
			if r.LifecycleState != domain.LifecycleStateCreating {
				t.Errorf("expected lifecycle_state creating, got %s", r.LifecycleState)
			}
			if r.CurrentConfigState != nil {
				t.Error("expected current_config_state to be nil on create")
			}
			if r.TargetVersion != 1 {
				t.Errorf("expected target_version 1, got %d", r.TargetVersion)
			}
			return nil
		})

	customerRequestRepo.EXPECT().
		Create(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, r *domain.CustomerRequest) error {
			if r.ID == "" {
				t.Error("expected request ID to be generated")
			}
			if r.RequestType != domain.CustomerRequestTypeCreate {
				t.Errorf("expected request_type create, got %s", r.RequestType)
			}
			if r.Status != domain.CustomerRequestStatusUnscheduled {
				t.Errorf("expected status unscheduled, got %s", r.Status)
			}
			if r.GoalConfigSnapshot["sku"] != "standard" {
				t.Errorf("expected goal snapshot sku=standard, got %v", r.GoalConfigSnapshot["sku"])
			}
			if r.Version != 1 {
				t.Errorf("expected version 1 to match resource instance target version, got %d", r.Version)
			}
			return nil
		})

	schedulerRepo.EXPECT().
		UpsertJobAndSchedule(gomock.Any(), gomock.Any()).
		Return("", int64(1), true, nil)

	schedulerRepo.EXPECT().
		SupercedeOlderRequests(gomock.Any(), gomock.Any(), int64(1)).
		Return(nil)

	instance, err := svc.CreateResource(context.Background(), "corr-1", "database", "tenant-1", initialState)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if instance.ID == "" {
		t.Error("expected returned instance to have an ID")
	}
}

func TestReconcileResource(t *testing.T) {
	ctrl := gomock.NewController(t)
	resourceInstanceRepo := mocks.NewMockResourceInstanceRepository(ctrl)
	customerRequestRepo := mocks.NewMockCustomerRequestRepository(ctrl)
	schedulerRepo := mocks.NewMockSchedulerRepository(ctrl)
	svc := service.NewLifecycleService(resourceInstanceRepo, customerRequestRepo, schedulerRepo)

	goalState := domain.ResourceState{"sku": "premium"}

	resourceInstanceRepo.EXPECT().
		UpdateGoalStateAndIncrementVersion(gomock.Any(), "instance-1", goalState,
			domain.LifecycleStateDeleting, domain.LifecycleStateDeleted).
		Return(int64(2), nil)

	customerRequestRepo.EXPECT().
		Create(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, r *domain.CustomerRequest) error {
			if r.RequestType != domain.CustomerRequestTypeReconcile {
				t.Errorf("expected request_type reconcile, got %s", r.RequestType)
			}
			if r.Version != 2 {
				t.Errorf("expected version 2 from incremented resource, got %d", r.Version)
			}
			if r.GoalConfigSnapshot["sku"] != "premium" {
				t.Errorf("expected goal snapshot sku=premium, got %v", r.GoalConfigSnapshot["sku"])
			}
			return nil
		})

	schedulerRepo.EXPECT().
		UpsertJobAndSchedule(gomock.Any(), gomock.Any()).
		Return("instance-1", int64(2), true, nil)

	schedulerRepo.EXPECT().
		SupercedeOlderRequests(gomock.Any(), "instance-1", int64(2)).
		Return(nil)

	if err := svc.ReconcileResource(context.Background(), "corr-2", "instance-1", goalState); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestDeleteResource(t *testing.T) {
	ctrl := gomock.NewController(t)
	resourceInstanceRepo := mocks.NewMockResourceInstanceRepository(ctrl)
	customerRequestRepo := mocks.NewMockCustomerRequestRepository(ctrl)
	schedulerRepo := mocks.NewMockSchedulerRepository(ctrl)
	svc := service.NewLifecycleService(resourceInstanceRepo, customerRequestRepo, schedulerRepo)

	resourceInstanceRepo.EXPECT().
		UpdateLifecycleStateAndIncrementVersion(gomock.Any(), "instance-1", domain.LifecycleStateDeleting,
			domain.LifecycleStateDeleted).
		Return(int64(3), nil)

	customerRequestRepo.EXPECT().
		Create(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, r *domain.CustomerRequest) error {
			if r.RequestType != domain.CustomerRequestTypeDelete {
				t.Errorf("expected request_type delete, got %s", r.RequestType)
			}
			if r.Version != 3 {
				t.Errorf("expected version 3, got %d", r.Version)
			}
			if r.Status != domain.CustomerRequestStatusUnscheduled {
				t.Errorf("expected status unscheduled, got %s", r.Status)
			}
			return nil
		})

	schedulerRepo.EXPECT().
		UpsertJobAndSchedule(gomock.Any(), gomock.Any()).
		Return("instance-1", int64(3), true, nil)

	schedulerRepo.EXPECT().
		SupercedeOlderRequests(gomock.Any(), "instance-1", int64(3)).
		Return(nil)

	if err := svc.DeleteResource(context.Background(), "corr-3", "instance-1"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestGetResourceState(t *testing.T) {
	ctrl := gomock.NewController(t)
	resourceInstanceRepo := mocks.NewMockResourceInstanceRepository(ctrl)
	customerRequestRepo := mocks.NewMockCustomerRequestRepository(ctrl)
	schedulerRepo := mocks.NewMockSchedulerRepository(ctrl)
	svc := service.NewLifecycleService(resourceInstanceRepo, customerRequestRepo, schedulerRepo)

	expected := &domain.ResourceInstance{
		ID:                 "instance-1",
		TenantID:           "tenant-1",
		ResourceType:       "database",
		CurrentConfigState: domain.ResourceState{"sku": "standard"},
		ConfigGoalState:    domain.ResourceState{"sku": "premium"},
		LifecycleState:     domain.LifecycleStateReconciling,
	}

	resourceInstanceRepo.EXPECT().
		Get(gomock.Any(), "instance-1").
		Return(expected, nil)

	instance, err := svc.GetResourceState(context.Background(), "instance-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if instance.LifecycleState != domain.LifecycleStateReconciling {
		t.Errorf("expected lifecycle_state reconciling, got %s", instance.LifecycleState)
	}
	if instance.CurrentConfigState["sku"] != "standard" {
		t.Errorf("expected current sku=standard, got %v", instance.CurrentConfigState["sku"])
	}
	if instance.ConfigGoalState["sku"] != "premium" {
		t.Errorf("expected goal sku=premium, got %v", instance.ConfigGoalState["sku"])
	}
}
