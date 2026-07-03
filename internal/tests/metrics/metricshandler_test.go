package metrics_test

import (
	"io"
	"log/slog"
	"testing"

	boundflowv1 "github.com/boundflow/boundflow/gen/boundflow/v1"
	"github.com/boundflow/boundflow/internal/metrics"
)

func i32(v int32) *int32     { return &v }
func f64(v float64) *float64 { return &v }

func newTestHandler() *metrics.MetricsHandler {
	return metrics.NewMetricsHandler(nil, nil, nil, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
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
