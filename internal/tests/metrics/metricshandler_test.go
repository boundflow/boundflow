package metrics_test

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"testing"

	"go.uber.org/mock/gomock"

	boundflowv1 "github.com/boundflow/boundflow/gen/boundflow/v1"
	"github.com/boundflow/boundflow/internal/domain"
	"github.com/boundflow/boundflow/internal/metrics"
	"github.com/boundflow/boundflow/internal/storage/mocks"
)

func i32(v int32) *int32     { return &v }
func f64(v float64) *float64 { return &v }

var discardLogger = slog.New(slog.NewTextHandler(io.Discard, nil))

func newTestHandler() *metrics.MetricsHandler {
	return metrics.NewMetricsHandler(nil, nil, nil, nil, discardLogger)
}

func snapshotsWithIDs(n int) []domain.WorkflowInvocationSnapshot {
	s := make([]domain.WorkflowInvocationSnapshot, n)
	for i := range s {
		s[i] = domain.WorkflowInvocationSnapshot{RequestID: fmt.Sprintf("run-%d", i)}
	}
	return s
}

func agentSnapshotsWithIDs(n int) []domain.AgentInvocationSnapshot {
	s := make([]domain.AgentInvocationSnapshot, n)
	for i := range s {
		s[i] = domain.AgentInvocationSnapshot{RequestID: fmt.Sprintf("run-%d", i)}
	}
	return s
}

func TestMergeAgentMetrics_CarriesFreshIntoEmpty(t *testing.T) {
	h := newTestHandler()
	job := map[string]*boundflowv1.AgentInvocationMetrics{}
	op := map[string]*boundflowv1.AgentInvocationMetrics{
		"a": {CostUsd: f64(1.5), LlmCalls: i32(2)},
	}

	h.MergeAgentMetrics(op, &job)

	got, ok := job["a"]
	if !ok {
		t.Fatal("expected agent a in job metrics")
	}
	if got.GetCostUsd() != 1.5 || got.GetLlmCalls() != 2 {
		t.Errorf("unexpected carried metrics: cost=%v llm=%v", got.GetCostUsd(), got.GetLlmCalls())
	}
}

func TestMergeAgentMetrics_SumsExistingAgent(t *testing.T) {
	h := newTestHandler()
	job := map[string]*boundflowv1.AgentInvocationMetrics{
		"a": {CostUsd: f64(1), LlmCalls: i32(3), ToolFailureCounts: map[string]int32{"x": 1}},
	}
	op := map[string]*boundflowv1.AgentInvocationMetrics{
		"a": {CostUsd: f64(2), LlmCalls: i32(4), ToolFailureCounts: map[string]int32{"x": 2, "y": 5}},
	}

	h.MergeAgentMetrics(op, &job)

	a := job["a"]
	if a.GetCostUsd() != 3 {
		t.Errorf("expected cost 3, got %v", a.GetCostUsd())
	}
	if a.GetLlmCalls() != 7 {
		t.Errorf("expected llm 7, got %v", a.GetLlmCalls())
	}
	if a.ToolFailureCounts["x"] != 3 || a.ToolFailureCounts["y"] != 5 {
		t.Errorf("expected tool counts x=3 y=5, got %v", a.ToolFailureCounts)
	}
}

func TestMergeAgentMetrics_NilFieldStaysNil_EmittedCarries(t *testing.T) {
	h := newTestHandler()
	// existing emitted only cost; op emits only llm_calls. Result should have both,
	// and a field neither emitted (failures) must stay nil.
	job := map[string]*boundflowv1.AgentInvocationMetrics{
		"a": {CostUsd: f64(1)},
	}
	op := map[string]*boundflowv1.AgentInvocationMetrics{
		"a": {LlmCalls: i32(2)},
	}

	h.MergeAgentMetrics(op, &job)

	a := job["a"]
	if a.CostUsd == nil || a.GetCostUsd() != 1 {
		t.Errorf("expected cost preserved at 1, got %v", a.CostUsd)
	}
	if a.LlmCalls == nil || a.GetLlmCalls() != 2 {
		t.Errorf("expected llm carried to 2, got %v", a.LlmCalls)
	}
	if a.Failures != nil {
		t.Errorf("expected failures to stay nil (neither side emitted), got %v", *a.Failures)
	}
}

func TestMergeAgentMetrics_NilOpMetricSkipped(t *testing.T) {
	h := newTestHandler()
	job := map[string]*boundflowv1.AgentInvocationMetrics{}
	op := map[string]*boundflowv1.AgentInvocationMetrics{"a": nil}

	h.MergeAgentMetrics(op, &job)

	if _, ok := job["a"]; ok {
		t.Error("expected nil op metric to be skipped, not added")
	}
}

func TestMergeAgentMetrics_MultipleAgentsIndependent(t *testing.T) {
	h := newTestHandler()
	job := map[string]*boundflowv1.AgentInvocationMetrics{
		"a": {CostUsd: f64(1)},
	}
	op := map[string]*boundflowv1.AgentInvocationMetrics{
		"a": {CostUsd: f64(1)},
		"b": {CostUsd: f64(9)},
	}

	h.MergeAgentMetrics(op, &job)

	if job["a"].GetCostUsd() != 2 {
		t.Errorf("agent a: expected 2, got %v", job["a"].GetCostUsd())
	}
	if job["b"].GetCostUsd() != 9 {
		t.Errorf("agent b: expected fresh 9, got %v", job["b"].GetCostUsd())
	}
}

// --- HandleAgentMetrics: history trimming ---

func TestHandleAgentMetrics_TrimsWorkflowHistoryToWindow(t *testing.T) {
	ctrl := gomock.NewController(t)
	versionMetricsRepo := mocks.NewMockVersionMetricsRepository(ctrl)
	agentStateRepo := mocks.NewMockAgentStateRepository(ctrl)
	metricsRepo := mocks.NewMockMetricsRepository(ctrl)
	h := metrics.NewMetricsHandler(nil, agentStateRepo, versionMetricsRepo, metricsRepo, discardLogger)

	wf := &domain.Workflow{
		ID: "wf-1", CurrentWorkflowVersion: 1, CurrentVersion: 1,
		InvocationMetrics: snapshotsWithIDs(domain.MaxLifecycleWindow),
	}

	versionMetricsRepo.EXPECT().GetCurrentVersionMetrics(gomock.Any(), "wf-1", 1).
		Return(&domain.WorkflowVersionMetrics{ToolFailureCounts: map[string]int{}}, nil)
	agentStateRepo.EXPECT().GetAllForWorkflow(gomock.Any(), "wf-1").
		Return(map[string]*domain.AgentState{}, nil)

	var captured []domain.WorkflowInvocationSnapshot
	metricsRepo.EXPECT().
		EmitMetrics(gomock.Any(), "wf-1", int64(1), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ string, _ int64, rolling []domain.WorkflowInvocationSnapshot, _ *domain.WorkflowVersionMetrics, _ map[string][]domain.AgentInvocationSnapshot) (bool, error) {
			captured = rolling
			return true, nil
		})

	err, _ := h.HandleAgentMetrics(context.Background(), "run-new", map[string]*boundflowv1.AgentInvocationMetrics{}, domain.WorkflowJobMetrics{}, wf)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(captured) != domain.MaxLifecycleWindow {
		t.Fatalf("expected %d entries, got %d", domain.MaxLifecycleWindow, len(captured))
	}
	if got := captured[0].RequestID; got != "run-1" {
		t.Errorf("expected oldest surviving entry to be run-1 (run-0 dropped), got %s", got)
	}
	if got := captured[len(captured)-1].RequestID; got != "run-new" {
		t.Errorf("expected newest entry to be run-new, got %s", got)
	}
}

func TestHandleAgentMetrics_TrimsAgentHistoryToWindow(t *testing.T) {
	ctrl := gomock.NewController(t)
	versionMetricsRepo := mocks.NewMockVersionMetricsRepository(ctrl)
	agentStateRepo := mocks.NewMockAgentStateRepository(ctrl)
	metricsRepo := mocks.NewMockMetricsRepository(ctrl)
	h := metrics.NewMetricsHandler(nil, agentStateRepo, versionMetricsRepo, metricsRepo, discardLogger)

	wf := &domain.Workflow{ID: "wf-1", CurrentWorkflowVersion: 1, CurrentVersion: 1}

	versionMetricsRepo.EXPECT().GetCurrentVersionMetrics(gomock.Any(), "wf-1", 1).
		Return(&domain.WorkflowVersionMetrics{ToolFailureCounts: map[string]int{}}, nil)
	agentStateRepo.EXPECT().GetAllForWorkflow(gomock.Any(), "wf-1").
		Return(map[string]*domain.AgentState{
			"analyst": {AgentName: "analyst", InvocationMetrics: agentSnapshotsWithIDs(domain.MaxLifecycleWindow)},
		}, nil)

	var captured map[string][]domain.AgentInvocationSnapshot
	metricsRepo.EXPECT().
		EmitMetrics(gomock.Any(), "wf-1", int64(1), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ string, _ int64, _ []domain.WorkflowInvocationSnapshot, _ *domain.WorkflowVersionMetrics, agentMetrics map[string][]domain.AgentInvocationSnapshot) (bool, error) {
			captured = agentMetrics
			return true, nil
		})

	err, _ := h.HandleAgentMetrics(context.Background(), "run-new", map[string]*boundflowv1.AgentInvocationMetrics{
		"analyst": {CostUsd: f64(0.01)},
	}, domain.WorkflowJobMetrics{}, wf)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	history := captured["analyst"]
	if len(history) != domain.MaxLifecycleWindow {
		t.Fatalf("expected %d entries, got %d", domain.MaxLifecycleWindow, len(history))
	}
	if got := history[0].RequestID; got != "run-1" {
		t.Errorf("expected oldest surviving entry to be run-1 (run-0 dropped), got %s", got)
	}
	if got := history[len(history)-1].RequestID; got != "run-new" {
		t.Errorf("expected newest entry to be run-new, got %s", got)
	}
}
