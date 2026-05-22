package service_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"

	"go.uber.org/mock/gomock"

	"github.com/convergeplane/convergeplane/internal/domain"
	"github.com/convergeplane/convergeplane/internal/service"
	"github.com/convergeplane/convergeplane/internal/storage/mocks"
)

var discardLogger = slog.New(slog.NewTextHandler(io.Discard, nil))

// mockRequestScheduler is a simple test double for service.RequestScheduler.
type mockRequestScheduler struct {
	scheduleErr error
}

func (m *mockRequestScheduler) ScheduleRequest(_ context.Context, _ string) error {
	return m.scheduleErr
}

// policy used in all tests — non-zero so resolveJobPolicy returns immediately.
var testPolicy = domain.JobPolicy{OperationTimeoutSeconds: 30}

func newSvc(ctrl *gomock.Controller) (*service.LifecycleService, *mocks.MockResourceInstanceRepository, *mocks.MockCustomerRequestRepository, *mocks.MockTenantRepository, *mocks.MockTenantGroupRepository, *mocks.MockAgentStateRepository) {
	resourceInstanceRepo := mocks.NewMockResourceInstanceRepository(ctrl)
	customerRequestRepo := mocks.NewMockCustomerRequestRepository(ctrl)
	tenantRepo := mocks.NewMockTenantRepository(ctrl)
	tenantGroupRepo := mocks.NewMockTenantGroupRepository(ctrl)
	agentStateRepo := mocks.NewMockAgentStateRepository(ctrl)
	sched := &mockRequestScheduler{}
	svc := service.NewLifecycleService(resourceInstanceRepo, customerRequestRepo, tenantRepo, tenantGroupRepo, agentStateRepo, sched, 10, discardLogger)
	return svc, resourceInstanceRepo, customerRequestRepo, tenantRepo, tenantGroupRepo, agentStateRepo
}

func TestCreateResource(t *testing.T) {
	ctrl := gomock.NewController(t)
	svc, resourceInstanceRepo, customerRequestRepo, _, _, _ := newSvc(ctrl)

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
			if r.JobPolicy.OperationTimeoutSeconds != 30 {
				t.Errorf("expected timeout 30, got %d", r.JobPolicy.OperationTimeoutSeconds)
			}
			return nil
		})

	instance, err := svc.CreateResource(context.Background(), "corr-1", "database", "tenant-1", initialState, testPolicy)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if instance.ID == "" {
		t.Error("expected returned instance to have an ID")
	}
}

func TestReconcileResource(t *testing.T) {
	ctrl := gomock.NewController(t)
	svc, resourceInstanceRepo, customerRequestRepo, _, _, _ := newSvc(ctrl)

	goalState := domain.ResourceState{"sku": "premium"}

	resourceInstanceRepo.EXPECT().
		Get(gomock.Any(), "instance-1").
		Return(&domain.ResourceInstance{ID: "instance-1", TenantID: "tenant-1"}, nil)

	resourceInstanceRepo.EXPECT().
		ReconcileGoalStateAndIncrementVersion(gomock.Any(), "instance-1", goalState,
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
			if r.JobPolicy.OperationTimeoutSeconds != 30 {
				t.Errorf("expected timeout 30, got %d", r.JobPolicy.OperationTimeoutSeconds)
			}
			return nil
		})

	if err := svc.ReconcileResource(context.Background(), "corr-2", "instance-1", goalState, testPolicy); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestDeleteResource(t *testing.T) {
	ctrl := gomock.NewController(t)
	svc, resourceInstanceRepo, customerRequestRepo, _, _, _ := newSvc(ctrl)

	resourceInstanceRepo.EXPECT().
		Get(gomock.Any(), "instance-1").
		Return(&domain.ResourceInstance{ID: "instance-1", TenantID: "tenant-1"}, nil)

	resourceInstanceRepo.EXPECT().
		UpdateLifecycleStateAndIncrementVersion(gomock.Any(), "instance-1", domain.LifecycleStateDeleting,
			domain.LifecycleStateDeleting, domain.LifecycleStateDeleted).
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
			if r.JobPolicy.OperationTimeoutSeconds != 30 {
				t.Errorf("expected timeout 30, got %d", r.JobPolicy.OperationTimeoutSeconds)
			}
			return nil
		})

	if err := svc.DeleteResource(context.Background(), "corr-3", "instance-1", testPolicy); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestGetResourceState(t *testing.T) {
	ctrl := gomock.NewController(t)
	svc, resourceInstanceRepo, _, _, _, _ := newSvc(ctrl)

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

// --- SetAgentRuntimePolicy ---

func TestSetAgentRuntimePolicy_DelegatesToRepo(t *testing.T) {
	ctrl := gomock.NewController(t)
	svc, _, _, _, _, agentStateRepo := newSvc(ctrl)

	policy := map[string]any{"model": "claude-sonnet-4-6", "max_tokens": float64(4096)}

	agentStateRepo.EXPECT().
		UpsertRuntimePolicy(gomock.Any(), "instance-1", "my_agent", policy).
		Return(nil)

	if err := svc.SetAgentRuntimePolicy(context.Background(), "instance-1", "my_agent", policy); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestSetAgentRuntimePolicy_RepoError(t *testing.T) {
	ctrl := gomock.NewController(t)
	svc, _, _, _, _, agentStateRepo := newSvc(ctrl)

	agentStateRepo.EXPECT().
		UpsertRuntimePolicy(gomock.Any(), "instance-1", "my_agent", gomock.Any()).
		Return(errors.New("db error"))

	if err := svc.SetAgentRuntimePolicy(context.Background(), "instance-1", "my_agent", map[string]any{}); err == nil {
		t.Fatal("expected error, got nil")
	}
}

// --- SetAgentLifecyclePolicy ---

func TestSetAgentLifecyclePolicy_DelegatesToRepo(t *testing.T) {
	ctrl := gomock.NewController(t)
	svc, _, _, _, _, agentStateRepo := newSvc(ctrl)

	policy := map[string]any{
		"rules": []any{
			map[string]any{"metric": "error_count", "window": float64(10), "threshold": float64(5), "operator": "gte"},
		},
	}

	agentStateRepo.EXPECT().
		UpsertLifecyclePolicy(gomock.Any(), "instance-1", "my_agent", policy).
		Return(nil)

	if err := svc.SetAgentLifecyclePolicy(context.Background(), "instance-1", "my_agent", policy); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestSetAgentLifecyclePolicy_RepoError(t *testing.T) {
	ctrl := gomock.NewController(t)
	svc, _, _, _, _, agentStateRepo := newSvc(ctrl)

	agentStateRepo.EXPECT().
		UpsertLifecyclePolicy(gomock.Any(), "instance-1", "my_agent", gomock.Any()).
		Return(errors.New("db error"))

	if err := svc.SetAgentLifecyclePolicy(context.Background(), "instance-1", "my_agent", map[string]any{}); err == nil {
		t.Fatal("expected error, got nil")
	}
}

// --- DeleteAgent ---

func TestDeleteAgent_DelegatesToRepo(t *testing.T) {
	ctrl := gomock.NewController(t)
	svc, _, _, _, _, agentStateRepo := newSvc(ctrl)

	agentStateRepo.EXPECT().
		Delete(gomock.Any(), "instance-1", "my_agent").
		Return(nil)

	if err := svc.DeleteAgent(context.Background(), "instance-1", "my_agent"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestDeleteAgent_RepoError(t *testing.T) {
	ctrl := gomock.NewController(t)
	svc, _, _, _, _, agentStateRepo := newSvc(ctrl)

	agentStateRepo.EXPECT().
		Delete(gomock.Any(), "instance-1", "my_agent").
		Return(errors.New("db error"))

	if err := svc.DeleteAgent(context.Background(), "instance-1", "my_agent"); err == nil {
		t.Fatal("expected error, got nil")
	}
}
