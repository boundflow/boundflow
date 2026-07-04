package service_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"

	"go.uber.org/mock/gomock"

	"github.com/boundflow/boundflow/internal/domain"
	"github.com/boundflow/boundflow/internal/service"
	"github.com/boundflow/boundflow/internal/storage/mocks"
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

func (m *mockApprovalResolver) ApproveJob(_ context.Context, _ string, _ string) (bool, domain.ResolvedApproval, error) {
	return m.approveResult, domain.ResolvedApproval{}, m.approveErr
}

func (m *mockApprovalResolver) RejectJob(_ context.Context, _ string, _ string) (bool, domain.ResolvedApproval, error) {
	return m.rejectResult, domain.ResolvedApproval{}, m.rejectErr
}

// policy used in all tests — non-zero so resolveJobPolicy returns immediately.
var testPolicy = domain.WorkflowRuntimeParams{OperationTimeoutSeconds: 30}

func newSvc(ctrl *gomock.Controller) (*service.LifecycleService, *mocks.MockWorkflowRepository, *mocks.MockCustomerRequestRepository, *mocks.MockTenantRepository, *mocks.MockTenantGroupRepository, *mocks.MockAgentStateRepository) {
	return newSvcWithApproval(ctrl, &mockApprovalResolver{approveResult: true, rejectResult: true})
}

func newSvcWithApproval(ctrl *gomock.Controller, approval service.ApprovalResolver) (*service.LifecycleService, *mocks.MockWorkflowRepository, *mocks.MockCustomerRequestRepository, *mocks.MockTenantRepository, *mocks.MockTenantGroupRepository, *mocks.MockAgentStateRepository) {
	workflowRepo := mocks.NewMockWorkflowRepository(ctrl)
	customerRequestRepo := mocks.NewMockCustomerRequestRepository(ctrl)
	tenantRepo := mocks.NewMockTenantRepository(ctrl)
	tenantGroupRepo := mocks.NewMockTenantGroupRepository(ctrl)
	agentStateRepo := mocks.NewMockAgentStateRepository(ctrl)
	modelPricingRepo := mocks.NewMockModelPricingRepository(ctrl)
	auditRepo := mocks.NewMockAuditRepository(ctrl)
	sched := &mockRequestScheduler{}
	// Permissive defaults for the pricing snapshot taken during invoke/invoke;
	// tests that don't assert on pricing are unaffected.
	workflowRepo.EXPECT().TenantGroupIDForWorkflow(gomock.Any(), gomock.Any()).Return("test-group", nil).AnyTimes()
	modelPricingRepo.EXPECT().ListDefaults(gomock.Any()).Return(nil, nil).AnyTimes()
	modelPricingRepo.EXPECT().ListForTenantGroup(gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
	// Approval decisions append an audit row on success; tests don't assert on it.
	auditRepo.EXPECT().Append(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	svc := service.NewLifecycleService(workflowRepo, customerRequestRepo, tenantRepo, tenantGroupRepo, agentStateRepo, modelPricingRepo, sched, approval, auditRepo, 10, 30, discardLogger)
	return svc, workflowRepo, customerRequestRepo, tenantRepo, tenantGroupRepo, agentStateRepo
}

func TestCreateWorkflow(t *testing.T) {
	ctrl := gomock.NewController(t)
	svc, workflowRepo, _, _, _, _ := newSvc(ctrl)

	cfg := domain.WorkflowConfig{Triggerable: true}

	workflowRepo.EXPECT().
		Create(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, r *domain.Workflow) error {
			if r.ID == "" {
				t.Error("expected ID to be generated")
			}
			if r.TenantID != "tenant-1" {
				t.Errorf("expected tenant_id tenant-1, got %s", r.TenantID)
			}
			if r.WorkflowType != "database" {
				t.Errorf("expected workflow_type database, got %s", r.WorkflowType)
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

	instance, err := svc.CreateWorkflow(context.Background(), "corr-1", "database", "tenant-1", cfg, 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if instance.ID == "" {
		t.Error("expected returned instance to have an ID")
	}
}

func TestInvokeWorkflow(t *testing.T) {
	ctrl := gomock.NewController(t)
	svc, workflowRepo, customerRequestRepo, _, _, agentStateRepo := newSvc(ctrl)

	workflowRepo.EXPECT().
		Get(gomock.Any(), "instance-1").
		Return(&domain.Workflow{
			ID:                     "instance-1",
			TenantID:               "tenant-1",
			CurrentWorkflowVersion: 1,
			WorkflowConfig:         domain.WorkflowConfig{InvokeTimeoutSeconds: 60, Triggerable: true},
			WorkflowState:          domain.WorkflowStateActive,
		}, nil)

	agentStateRepo.EXPECT().
		GetAllForWorkflow(gomock.Any(), "instance-1").
		Return(nil, nil)

	customerRequestRepo.EXPECT().
		CreateInvocationRequest(gomock.Any(), gomock.Any(),
			[]domain.LifecycleState{domain.LifecycleStateDeleting, domain.LifecycleStateDeleted}).
		DoAndReturn(func(_ context.Context, r *domain.CustomerRequest, _ []domain.LifecycleState) (int64, error) {
			if r.RequestType != domain.CustomerRequestTypeInvoke {
				t.Errorf("expected request_type invoke, got %s", r.RequestType)
			}
			if r.RequestInfo["operationTimeoutSeconds"] != 30 {
				t.Errorf("expected timeout 30 in requestInfo, got %v", r.RequestInfo["operationTimeoutSeconds"])
			}
			return 2, nil
		})

	requestID, err := svc.InvokeWorkflow(context.Background(), "corr-2", "instance-1", testPolicy)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if requestID == "" {
		t.Fatal("InvokeWorkflow should return the created request id")
	}
}

func TestDeleteWorkflow(t *testing.T) {
	ctrl := gomock.NewController(t)
	svc, workflowRepo, _, _, _, _ := newSvc(ctrl)

	workflowRepo.EXPECT().
		Get(gomock.Any(), "instance-1").
		Return(&domain.Workflow{ID: "instance-1", TenantID: "tenant-1"}, nil)

	workflowRepo.EXPECT().
		MarkDeleted(gomock.Any(), "instance-1").
		Return(nil)

	if err := svc.DeleteWorkflow(context.Background(), "corr-3", "instance-1"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestResolveInterruptedWorkflow(t *testing.T) {
	ctrl := gomock.NewController(t)
	svc, workflowRepo, _, _, _, _ := newSvc(ctrl)

	workflowRepo.EXPECT().
		ResolveInterruptedWorkflow(gomock.Any(), "instance-1", "req-9").
		Return(true, nil)

	if err := svc.ResolveInterruptedWorkflow(context.Background(), "instance-1", "req-9"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestResolveInterruptedWorkflow_GuardMismatch_ReturnsErrNotInterrupted(t *testing.T) {
	ctrl := gomock.NewController(t)
	svc, workflowRepo, _, _, _, _ := newSvc(ctrl)

	// Guard didn't match (wrong request id or not interrupted) → repo reports false.
	workflowRepo.EXPECT().
		ResolveInterruptedWorkflow(gomock.Any(), "instance-1", "wrong-req").
		Return(false, nil)

	err := svc.ResolveInterruptedWorkflow(context.Background(), "instance-1", "wrong-req")
	if !errors.Is(err, service.ErrNotInterrupted) {
		t.Fatalf("expected ErrNotInterrupted, got %v", err)
	}
}

func TestResolveInterruptedWorkflow_RepoError_Propagates(t *testing.T) {
	ctrl := gomock.NewController(t)
	svc, workflowRepo, _, _, _, _ := newSvc(ctrl)

	workflowRepo.EXPECT().
		ResolveInterruptedWorkflow(gomock.Any(), "instance-1", "req-9").
		Return(false, errors.New("db down"))

	if err := svc.ResolveInterruptedWorkflow(context.Background(), "instance-1", "req-9"); err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestGetWorkflow(t *testing.T) {
	ctrl := gomock.NewController(t)
	svc, workflowRepo, _, _, _, _ := newSvc(ctrl)

	expected := &domain.Workflow{
		ID:                     "instance-1",
		TenantID:               "tenant-1",
		WorkflowType:           "database",
		CurrentWorkflowVersion: 2,
		WorkflowConfig:         domain.WorkflowConfig{Triggerable: true},
		LifecycleState:         domain.LifecycleStateInvoking,
	}

	workflowRepo.EXPECT().
		Get(gomock.Any(), "instance-1").
		Return(expected, nil)

	instance, err := svc.GetWorkflow(context.Background(), "instance-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if instance.LifecycleState != domain.LifecycleStateInvoking {
		t.Errorf("expected lifecycle_state invoking, got %s", instance.LifecycleState)
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

	if err := svc.ApproveWorkflow(context.Background(), "instance-1", "approval-id-1", "tester"); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
}

func TestApproveWorkflow_IdMismatch_ReturnsInvalidWorkflowState(t *testing.T) {
	ctrl := gomock.NewController(t)
	svc, _, _, _, _, _ := newSvcWithApproval(ctrl, &mockApprovalResolver{approveResult: false})

	err := svc.ApproveWorkflow(context.Background(), "instance-1", "wrong-id", "tester")
	if !errors.Is(err, service.ErrInvalidWorkflowState) {
		t.Fatalf("expected ErrInvalidWorkflowState, got %v", err)
	}
}

func TestApproveWorkflow_StorageError_ReturnsError(t *testing.T) {
	ctrl := gomock.NewController(t)
	storageErr := errors.New("db error")
	svc, _, _, _, _, _ := newSvcWithApproval(ctrl, &mockApprovalResolver{approveErr: storageErr})

	if err := svc.ApproveWorkflow(context.Background(), "instance-1", "approval-id-1", "tester"); err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestRejectWorkflow_Success(t *testing.T) {
	ctrl := gomock.NewController(t)
	svc, _, _, _, _, _ := newSvcWithApproval(ctrl, &mockApprovalResolver{rejectResult: true})

	if err := svc.RejectWorkflow(context.Background(), "instance-1", "approval-id-1", "tester"); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
}

func TestRejectWorkflow_IdMismatch_ReturnsInvalidWorkflowState(t *testing.T) {
	ctrl := gomock.NewController(t)
	svc, _, _, _, _, _ := newSvcWithApproval(ctrl, &mockApprovalResolver{rejectResult: false})

	err := svc.RejectWorkflow(context.Background(), "instance-1", "wrong-id", "tester")
	if !errors.Is(err, service.ErrInvalidWorkflowState) {
		t.Fatalf("expected ErrInvalidWorkflowState, got %v", err)
	}
}

func TestRejectWorkflow_StorageError_ReturnsError(t *testing.T) {
	ctrl := gomock.NewController(t)
	storageErr := errors.New("db error")
	svc, _, _, _, _, _ := newSvcWithApproval(ctrl, &mockApprovalResolver{rejectErr: storageErr})

	if err := svc.RejectWorkflow(context.Background(), "instance-1", "approval-id-1", "tester"); err == nil {
		t.Fatal("expected error, got nil")
	}
}
