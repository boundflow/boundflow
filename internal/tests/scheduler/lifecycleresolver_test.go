package scheduler_test

import (
	"context"
	"testing"

	"go.uber.org/mock/gomock"

	"github.com/boundflow/boundflow/internal/domain"
	"github.com/boundflow/boundflow/internal/scheduler"
	"github.com/boundflow/boundflow/internal/storage/mocks"
)

func TestResolveLifecyclePolicy_SetVersionGuardsOnPreviousVersion(t *testing.T) {
	ctrl := gomock.NewController(t)
	resolverRepo := mocks.NewMockLifecycleResolverRepository(ctrl)
	workflowRepo := mocks.NewMockWorkflowRepository(ctrl)
	versionMetricsRepo := mocks.NewMockVersionMetricsRepository(ctrl)
	resolver := scheduler.NewLifecycleResolver(30, engineLogger, resolverRepo, workflowRepo, versionMetricsRepo)

	workflow := &domain.Workflow{
		ID:                     "wf-1",
		CurrentWorkflowVersion: 2,
		CurrentVersion:         5,
		WorkflowState:          domain.WorkflowStateActive,
		LifecyclePolicy: domain.WorkflowLifecyclePolicy{
			Rules: []domain.WorkflowLifecyclePolicyRule{setVersionRule(domain.WorkflowMetricNumFailures, 2, 1)},
		},
		InvocationMetrics: []domain.WorkflowInvocationSnapshot{{Failures: ip(2)}},
	}
	vm := &domain.WorkflowVersionMetrics{TotalFailures: 2}

	// A set_version resolution calls TryApplyVersionResolution, guarded on expectedVersion=2
	// (the version observed before deciding to fire) -- never TryApplyStateResolution.
	resolverRepo.EXPECT().
		TryApplyVersionResolution(gomock.Any(), "wf-1", int64(5), 2, 1).
		Return(true, nil)

	action, err := resolver.ResolveLifecyclePolicy(context.Background(), workflow, vm)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if action == nil || action.PreviousVersion != 2 {
		t.Errorf("expected an action with previous_version=2, got %+v", action)
	}
}

func TestResolveLifecyclePolicy_PauseUsesStateResolution(t *testing.T) {
	ctrl := gomock.NewController(t)
	resolverRepo := mocks.NewMockLifecycleResolverRepository(ctrl)
	workflowRepo := mocks.NewMockWorkflowRepository(ctrl)
	versionMetricsRepo := mocks.NewMockVersionMetricsRepository(ctrl)
	resolver := scheduler.NewLifecycleResolver(30, engineLogger, resolverRepo, workflowRepo, versionMetricsRepo)

	workflow := &domain.Workflow{
		ID:                     "wf-1",
		CurrentWorkflowVersion: 1,
		CurrentVersion:         3,
		WorkflowState:          domain.WorkflowStateActive,
		LifecyclePolicy: domain.WorkflowLifecyclePolicy{
			Rules: []domain.WorkflowLifecyclePolicyRule{pauseRule(domain.WorkflowMetricNumFailures, 2, 1)},
		},
		InvocationMetrics: []domain.WorkflowInvocationSnapshot{{Failures: ip(2)}},
	}

	// A pause/cooldown resolution calls TryApplyStateResolution -- never TryApplyVersionResolution,
	// so it structurally can't touch current_workflow_version.
	resolverRepo.EXPECT().
		TryApplyStateResolution(gomock.Any(), "wf-1", int64(3), domain.WorkflowStatePaused, gomock.Any()).
		Return(true, nil)

	action, err := resolver.ResolveLifecyclePolicy(context.Background(), workflow, &domain.WorkflowVersionMetrics{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if action == nil {
		t.Fatal("expected a policy action")
	}
}

func TestResolveLifecyclePolicy_NoRuleFiredUsesStateResolution(t *testing.T) {
	ctrl := gomock.NewController(t)
	resolverRepo := mocks.NewMockLifecycleResolverRepository(ctrl)
	workflowRepo := mocks.NewMockWorkflowRepository(ctrl)
	versionMetricsRepo := mocks.NewMockVersionMetricsRepository(ctrl)
	resolver := scheduler.NewLifecycleResolver(30, engineLogger, resolverRepo, workflowRepo, versionMetricsRepo)

	workflow := &domain.Workflow{
		ID:                     "wf-1",
		CurrentWorkflowVersion: 1,
		CurrentVersion:         3,
		WorkflowState:          domain.WorkflowStateActive,
		LifecyclePolicy:        domain.WorkflowLifecyclePolicy{Rules: nil},
		InvocationMetrics:      []domain.WorkflowInvocationSnapshot{{Failures: ip(0)}},
	}

	// No rule fires -> still resolves via TryApplyStateResolution (to advance
	// lifecycle_last_resolved), never TryApplyVersionResolution, and no action is reported.
	resolverRepo.EXPECT().
		TryApplyStateResolution(gomock.Any(), "wf-1", int64(3), domain.WorkflowStateActive, gomock.Any()).
		Return(true, nil)

	action, err := resolver.ResolveLifecyclePolicy(context.Background(), workflow, &domain.WorkflowVersionMetrics{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if action != nil {
		t.Errorf("expected no action when no rule fired, got %+v", action)
	}
}
