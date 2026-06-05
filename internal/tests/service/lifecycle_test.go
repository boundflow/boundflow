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

// mockApprovalResolver is a simple test double for service.ApprovalResolver.
type mockApprovalResolver struct {
	approveResult bool
	approveErr    error
	rejectResult  bool
	rejectErr     error
}

func (m *mockApprovalResolver) ApproveJob(_ context.Context, _ string, _ string) (bool, error) {
	return m.approveResult, m.approveErr
}

func (m *mockApprovalResolver) RejectJob(_ context.Context, _ string, _ string) (bool, error) {
	return m.rejectResult, m.rejectErr
}

// policy used in all tests — non-zero so resolveJobPolicy returns immediately.
var testPolicy = domain.WorkflowRuntimeParams{OperationTimeoutSeconds: 30}

func newSvc(ctrl *gomock.Controller) (*service.LifecycleService, *mocks.MockResourceInstanceRepository, *mocks.MockCustomerRequestRepository, *mocks.MockTenantRepository, *mocks.MockTenantGroupRepository, *mocks.MockAgentStateRepository) {
	return newSvcWithApproval(ctrl, &mockApprovalResolver{approveResult: true, rejectResult: true})
}

func newSvcWithApproval(ctrl *gomock.Controller, approval service.ApprovalResolver) (*service.LifecycleService, *mocks.MockResourceInstanceRepository, *mocks.MockCustomerRequestRepository, *mocks.MockTenantRepository, *mocks.MockTenantGroupRepository, *mocks.MockAgentStateRepository) {
	resourceInstanceRepo := mocks.NewMockResourceInstanceRepository(ctrl)
	customerRequestRepo := mocks.NewMockCustomerRequestRepository(ctrl)
	tenantRepo := mocks.NewMockTenantRepository(ctrl)
	tenantGroupRepo := mocks.NewMockTenantGroupRepository(ctrl)
	agentStateRepo := mocks.NewMockAgentStateRepository(ctrl)
	sched := &mockRequestScheduler{}
	svc := service.NewLifecycleService(resourceInstanceRepo, customerRequestRepo, tenantRepo, tenantGroupRepo, agentStateRepo, sched, approval, 10, discardLogger)
	return svc, resourceInstanceRepo, customerRequestRepo, tenantRepo, tenantGroupRepo, agentStateRepo
}

func TestCreateResource(t *testing.T) {
	ctrl := gomock.NewController(t)
	svc, resourceInstanceRepo, _, _, _, _ := newSvc(ctrl)

	cfg := domain.WorkflowConfig{Triggerable: true}

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
			if r.LifecycleState != domain.LifecycleStateActive {
				t.Errorf("expected lifecycle_state active, got %s", r.LifecycleState)
			}
			if r.WorkflowConfig != cfg {
				t.Errorf("expected workflow config %+v, got %+v", cfg, r.WorkflowConfig)
			}
			if r.CurrentWorkflowVersion != 1 {
				t.Errorf("expected current_workflow_version 1, got %d", r.CurrentWorkflowVersion)
			}
			if r.TargetVersion != 0 {
				t.Errorf("expected target_version 0, got %d", r.TargetVersion)
			}
			if r.CurrentVersion != 0 {
				t.Errorf("expected current_version 0, got %d", r.CurrentVersion)
			}
			return nil
		})

	instance, err := svc.CreateResource(context.Background(), "corr-1", "database", "tenant-1", cfg, 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if instance.ID == "" {
		t.Error("expected returned instance to have an ID")
	}
}

func TestReconcileResource(t *testing.T) {
	ctrl := gomock.NewController(t)
	svc, resourceInstanceRepo, customerRequestRepo, _, _, agentStateRepo := newSvc(ctrl)

	resourceInstanceRepo.EXPECT().
		Get(gomock.Any(), "instance-1").
		Return(&domain.ResourceInstance{
			ID:                     "instance-1",
			TenantID:               "tenant-1",
			CurrentWorkflowVersion: 1,
			WorkflowConfig:         domain.WorkflowConfig{InvokeTimeoutSeconds: 60, Triggerable: true},
			WorkflowState:          domain.WorkflowStateActive,
		}, nil)

	agentStateRepo.EXPECT().
		GetAllForResource(gomock.Any(), "instance-1").
		Return(nil, nil)

	customerRequestRepo.EXPECT().
		CreateInvocationRequest(gomock.Any(), gomock.Any(),
			[]domain.LifecycleState{domain.LifecycleStateDeleting, domain.LifecycleStateDeleted}).
		DoAndReturn(func(_ context.Context, r *domain.CustomerRequest, _ []domain.LifecycleState) (int64, error) {
			if r.RequestType != domain.CustomerRequestTypeReconcile {
				t.Errorf("expected request_type reconcile, got %s", r.RequestType)
			}
			if r.RequestInfo["operationTimeoutSeconds"] != 30 {
				t.Errorf("expected timeout 30 in requestInfo, got %v", r.RequestInfo["operationTimeoutSeconds"])
			}
			return 2, nil
		})

	if err := svc.ReconcileResource(context.Background(), "corr-2", "instance-1", testPolicy); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestDeleteResource(t *testing.T) {
	ctrl := gomock.NewController(t)
	svc, resourceInstanceRepo, _, _, _, _ := newSvc(ctrl)

	resourceInstanceRepo.EXPECT().
		Get(gomock.Any(), "instance-1").
		Return(&domain.ResourceInstance{ID: "instance-1", TenantID: "tenant-1"}, nil)

	resourceInstanceRepo.EXPECT().
		MarkDeleted(gomock.Any(), "instance-1").
		Return(nil)

	if err := svc.DeleteResource(context.Background(), "corr-3", "instance-1"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestGetResourceState(t *testing.T) {
	ctrl := gomock.NewController(t)
	svc, resourceInstanceRepo, _, _, _, _ := newSvc(ctrl)

	expected := &domain.ResourceInstance{
		ID:                     "instance-1",
		TenantID:               "tenant-1",
		ResourceType:           "database",
		CurrentWorkflowVersion: 2,
		WorkflowConfig:         domain.WorkflowConfig{Triggerable: true},
		LifecycleState:         domain.LifecycleStateReconciling,
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
	if instance.CurrentWorkflowVersion != 2 {
		t.Errorf("expected current_workflow_version 2, got %d", instance.CurrentWorkflowVersion)
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

func TestApproveWorkflow_Success(t *testing.T) {
	ctrl := gomock.NewController(t)
	svc, _, _, _, _, _ := newSvcWithApproval(ctrl, &mockApprovalResolver{approveResult: true})

	if err := svc.ApproveWorkflow(context.Background(), "instance-1", "approval-id-1"); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
}

func TestApproveWorkflow_IdMismatch_ReturnsInvalidWorkflowState(t *testing.T) {
	ctrl := gomock.NewController(t)
	svc, _, _, _, _, _ := newSvcWithApproval(ctrl, &mockApprovalResolver{approveResult: false})

	err := svc.ApproveWorkflow(context.Background(), "instance-1", "wrong-id")
	if !errors.Is(err, service.ErrInvalidWorkflowState) {
		t.Fatalf("expected ErrInvalidWorkflowState, got %v", err)
	}
}

func TestApproveWorkflow_StorageError_ReturnsError(t *testing.T) {
	ctrl := gomock.NewController(t)
	storageErr := errors.New("db error")
	svc, _, _, _, _, _ := newSvcWithApproval(ctrl, &mockApprovalResolver{approveErr: storageErr})

	if err := svc.ApproveWorkflow(context.Background(), "instance-1", "approval-id-1"); err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestRejectWorkflow_Success(t *testing.T) {
	ctrl := gomock.NewController(t)
	svc, _, _, _, _, _ := newSvcWithApproval(ctrl, &mockApprovalResolver{rejectResult: true})

	if err := svc.RejectWorkflow(context.Background(), "instance-1", "approval-id-1"); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
}

func TestRejectWorkflow_IdMismatch_ReturnsInvalidWorkflowState(t *testing.T) {
	ctrl := gomock.NewController(t)
	svc, _, _, _, _, _ := newSvcWithApproval(ctrl, &mockApprovalResolver{rejectResult: false})

	err := svc.RejectWorkflow(context.Background(), "instance-1", "wrong-id")
	if !errors.Is(err, service.ErrInvalidWorkflowState) {
		t.Fatalf("expected ErrInvalidWorkflowState, got %v", err)
	}
}

func TestRejectWorkflow_StorageError_ReturnsError(t *testing.T) {
	ctrl := gomock.NewController(t)
	storageErr := errors.New("db error")
	svc, _, _, _, _, _ := newSvcWithApproval(ctrl, &mockApprovalResolver{rejectErr: storageErr})

	if err := svc.RejectWorkflow(context.Background(), "instance-1", "approval-id-1"); err == nil {
		t.Fatal("expected error, got nil")
	}
}
