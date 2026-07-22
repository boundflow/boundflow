package handlers_test

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"go.uber.org/mock/gomock"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"

	boundflowv1 "github.com/boundflow/boundflow/gen/boundflow/v1"
	"github.com/boundflow/boundflow/internal/auth"
	"github.com/boundflow/boundflow/internal/domain"
	"github.com/boundflow/boundflow/internal/server/handlers"
	"github.com/boundflow/boundflow/internal/service"
	"github.com/boundflow/boundflow/internal/storage/mocks"
)

var discardLogger = slog.New(slog.NewTextHandler(io.Discard, nil))

const testTenantGroup = "test-group"

func newHandler(ctrl *gomock.Controller) (*handlers.WorkflowServiceHandler, *mocks.MockWorkflowRepository, *mocks.MockAgentStateRepository, *mocks.MockVersionMetricsRepository) {
	workflowRepo := mocks.NewMockWorkflowRepository(ctrl)
	agentStateRepo := mocks.NewMockAgentStateRepository(ctrl)
	tenantRepo := mocks.NewMockTenantRepository(ctrl)
	tenantGroupRepo := mocks.NewMockTenantGroupRepository(ctrl)
	modelPricingRepo := mocks.NewMockModelPricingRepository(ctrl)
	versionMetricsRepo := mocks.NewMockVersionMetricsRepository(ctrl)
	auditRepo := mocks.NewMockAuditRepository(ctrl)
	customerRequestRepo := mocks.NewMockCustomerRequestRepository(ctrl)

	workflowRepo.EXPECT().TenantGroupIDForWorkflow(gomock.Any(), gomock.Any()).Return(testTenantGroup, nil).AnyTimes()

	svc := service.NewLifecycleService(
		workflowRepo, customerRequestRepo, tenantRepo, tenantGroupRepo, agentStateRepo,
		modelPricingRepo, versionMetricsRepo, nil, nil, nil, auditRepo, 10, 30, discardLogger,
	)
	return handlers.NewWorkflowServiceHandler(svc), workflowRepo, agentStateRepo, versionMetricsRepo
}

func authedCtx() context.Context {
	return auth.WithTenantGroup(context.Background(), testTenantGroup)
}

func agentRuleProto(window int32) *structpb.Struct {
	s, err := structpb.NewStruct(map[string]any{
		"rules": []any{rule(float64(window))},
	})
	if err != nil {
		panic(err)
	}
	return s
}

func workflowRuleProto(window int32) *boundflowv1.WorkflowLifecyclePolicy {
	return &boundflowv1.WorkflowLifecyclePolicy{
		Rules: []*boundflowv1.WorkflowLifecyclePolicyRule{
			{
				Metric:    boundflowv1.WorkflowMetric_WORKFLOW_METRIC_NUM_FAILURES,
				Threshold: 3,
				Window:    window,
				Action:    &boundflowv1.WorkflowLifecyclePolicyAction{Type: boundflowv1.WorkflowPolicyActionType_WORKFLOW_POLICY_ACTION_PAUSE},
			},
		},
	}
}

func requireInvalidArgument(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("expected an error, got nil")
	}
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument, got %v", err)
	}
}

func rule(window any) map[string]any {
	return map[string]any{
		"metric":    "cost_usd",
		"op":        "greater_than",
		"threshold": 1.0,
		"window":    window,
		"action":    map[string]any{"field": "model", "value": "claude-haiku-4-5"},
	}
}

func TestValidateAgentLifecycleRules_NoRulesKey(t *testing.T) {
	if err := handlers.ValidateAgentLifecycleRules(map[string]any{}); err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
}

func TestValidateAgentLifecycleRules_ValidRule(t *testing.T) {
	policy := map[string]any{"rules": []any{rule(5.0)}}
	if err := handlers.ValidateAgentLifecycleRules(policy); err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
}

func TestValidateAgentLifecycleRules_AtMax(t *testing.T) {
	policy := map[string]any{"rules": []any{rule(float64(domain.MaxLifecycleWindow))}}
	if err := handlers.ValidateAgentLifecycleRules(policy); err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
}

func TestValidateAgentLifecycleRules_OverMax(t *testing.T) {
	policy := map[string]any{"rules": []any{rule(float64(domain.MaxLifecycleWindow + 1))}}
	if err := handlers.ValidateAgentLifecycleRules(policy); err == nil {
		t.Fatal("expected an error, got nil")
	}
}

func TestValidateAgentLifecycleRules_ZeroWindow(t *testing.T) {
	policy := map[string]any{"rules": []any{rule(0.0)}}
	if err := handlers.ValidateAgentLifecycleRules(policy); err == nil {
		t.Fatal("expected an error, got nil")
	}
}

func TestValidateAgentLifecycleRules_NegativeWindow(t *testing.T) {
	policy := map[string]any{"rules": []any{rule(-1.0)}}
	if err := handlers.ValidateAgentLifecycleRules(policy); err == nil {
		t.Fatal("expected an error, got nil")
	}
}

func TestValidateAgentLifecycleRules_MissingWindow(t *testing.T) {
	r := rule(5.0)
	delete(r, "window")
	policy := map[string]any{"rules": []any{r}}
	if err := handlers.ValidateAgentLifecycleRules(policy); err == nil {
		t.Fatal("expected an error, got nil")
	}
}

func TestValidateAgentLifecycleRules_WindowWrongType(t *testing.T) {
	policy := map[string]any{"rules": []any{rule("five")}}
	if err := handlers.ValidateAgentLifecycleRules(policy); err == nil {
		t.Fatal("expected an error, got nil")
	}
}

func TestValidateAgentLifecycleRules_RuleNotAnObject(t *testing.T) {
	policy := map[string]any{"rules": []any{"not-a-rule"}}
	if err := handlers.ValidateAgentLifecycleRules(policy); err == nil {
		t.Fatal("expected an error, got nil")
	}
}

func TestValidateAgentLifecycleRules_RulesNotAnArray(t *testing.T) {
	policy := map[string]any{"rules": "not-an-array"}
	if err := handlers.ValidateAgentLifecycleRules(policy); err == nil {
		t.Fatal("expected an error, got nil")
	}
}

// --- SetAgentLifecyclePolicy (handler, over the actual RPC request shape) ---

func TestSetAgentLifecyclePolicy_RejectsWindowOverMax(t *testing.T) {
	ctrl := gomock.NewController(t)
	h, _, _, _ := newHandler(ctrl)

	_, err := h.SetAgentLifecyclePolicy(authedCtx(), &boundflowv1.SetAgentLifecyclePolicyRequest{
		WorkflowId:      "wf-1",
		AgentName:       "analyst",
		LifecyclePolicy: agentRuleProto(domain.MaxLifecycleWindow + 1),
	})
	requireInvalidArgument(t, err)
}

func TestSetAgentLifecyclePolicy_RejectsZeroWindow(t *testing.T) {
	ctrl := gomock.NewController(t)
	h, _, _, _ := newHandler(ctrl)

	_, err := h.SetAgentLifecyclePolicy(authedCtx(), &boundflowv1.SetAgentLifecyclePolicyRequest{
		WorkflowId:      "wf-1",
		AgentName:       "analyst",
		LifecyclePolicy: agentRuleProto(0),
	})
	requireInvalidArgument(t, err)
}

func TestSetAgentLifecyclePolicy_AcceptsValidWindow(t *testing.T) {
	ctrl := gomock.NewController(t)
	h, _, agentStateRepo, _ := newHandler(ctrl)
	agentStateRepo.EXPECT().UpsertLifecyclePolicy(gomock.Any(), "wf-1", "analyst", gomock.Any()).Return(nil)

	_, err := h.SetAgentLifecyclePolicy(authedCtx(), &boundflowv1.SetAgentLifecyclePolicyRequest{
		WorkflowId:      "wf-1",
		AgentName:       "analyst",
		LifecyclePolicy: agentRuleProto(5),
	})
	if err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
}

// --- SetWorkflowLifecyclePolicy (handler, over the actual RPC request shape) ---

func TestSetWorkflowLifecyclePolicy_RejectsWindowOverMax(t *testing.T) {
	ctrl := gomock.NewController(t)
	h, _, _, _ := newHandler(ctrl)

	_, err := h.SetWorkflowLifecyclePolicy(authedCtx(), &boundflowv1.SetWorkflowLifecyclePolicyRequest{
		WorkflowId:      "wf-1",
		LifecyclePolicy: workflowRuleProto(domain.MaxLifecycleWindow + 1),
	})
	requireInvalidArgument(t, err)
}

func TestSetWorkflowLifecyclePolicy_RejectsZeroWindow(t *testing.T) {
	ctrl := gomock.NewController(t)
	h, _, _, _ := newHandler(ctrl)

	_, err := h.SetWorkflowLifecyclePolicy(authedCtx(), &boundflowv1.SetWorkflowLifecyclePolicyRequest{
		WorkflowId:      "wf-1",
		LifecyclePolicy: workflowRuleProto(0),
	})
	requireInvalidArgument(t, err)
}

func TestSetWorkflowLifecyclePolicy_AcceptsValidWindow(t *testing.T) {
	ctrl := gomock.NewController(t)
	h, workflowRepo, _, _ := newHandler(ctrl)
	workflowRepo.EXPECT().UpdateLifecyclePolicy(gomock.Any(), "wf-1", gomock.Any()).Return(nil)

	_, err := h.SetWorkflowLifecyclePolicy(authedCtx(), &boundflowv1.SetWorkflowLifecyclePolicyRequest{
		WorkflowId:      "wf-1",
		LifecyclePolicy: workflowRuleProto(5),
	})
	if err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
}

// --- GetWorkflowMetrics (handler, over the actual RPC request shape) ---

func TestGetWorkflowMetrics_RequiresWorkflowId(t *testing.T) {
	ctrl := gomock.NewController(t)
	h, _, _, _ := newHandler(ctrl)

	_, err := h.GetWorkflowMetrics(authedCtx(), &boundflowv1.GetWorkflowMetricsRequest{})
	requireInvalidArgument(t, err)
}

func TestGetWorkflowMetrics_ReturnsConvertedTotals(t *testing.T) {
	ctrl := gomock.NewController(t)
	h, workflowRepo, _, versionMetricsRepo := newHandler(ctrl)

	workflowRepo.EXPECT().Get(gomock.Any(), "wf-1").
		Return(&domain.Workflow{ID: "wf-1", CurrentWorkflowVersion: 3}, nil)
	versionMetricsRepo.EXPECT().GetCurrentVersionMetrics(gomock.Any(), "wf-1", 3).
		Return(&domain.WorkflowVersionMetrics{
			Version: 3, TotalCost: 2.5, RunCount: 10, TotalFailures: 2, TotalLLMCalls: 40,
			TotalLatencySeconds: 12.5, TotalApprovalRejections: 1,
			ToolFailureCounts: map[string]int{"refund_policy": 3},
		}, nil)

	resp, err := h.GetWorkflowMetrics(authedCtx(), &boundflowv1.GetWorkflowMetricsRequest{WorkflowId: "wf-1"})
	if err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
	if resp.Version != 3 || resp.TotalCost != 2.5 || resp.RunCount != 10 {
		t.Errorf("unexpected response: %+v", resp)
	}
	if resp.ToolFailureCounts["refund_policy"] != 3 {
		t.Errorf("expected tool_failure_counts to carry through, got %+v", resp.ToolFailureCounts)
	}
}
