package scheduler_test

import (
	"io"
	"log/slog"
	"testing"

	"github.com/boundflow/boundflow/internal/domain"
	"github.com/boundflow/boundflow/internal/scheduler"
)

var engineLogger = slog.New(slog.NewTextHandler(io.Discard, nil))

func fp(v float64) *float64 { return &v }
func ip(v int) *int         { return &v }

func cooldownRule(metric domain.WorkflowMetric, threshold float64, window, seconds int) domain.WorkflowLifecyclePolicyRule {
	return domain.WorkflowLifecyclePolicyRule{
		Metric:    metric,
		Threshold: threshold,
		Window:    window,
		Action:    domain.WorkflowLifecyclePolicyAction{Type: domain.WorkflowPolicyActionCooldown, CooldownSeconds: seconds},
	}
}

func pauseRule(metric domain.WorkflowMetric, threshold float64, window int) domain.WorkflowLifecyclePolicyRule {
	return domain.WorkflowLifecyclePolicyRule{
		Metric:    metric,
		Threshold: threshold,
		Window:    window,
		Action:    domain.WorkflowLifecyclePolicyAction{Type: domain.WorkflowPolicyActionPause},
	}
}

func setVersionRule(metric domain.WorkflowMetric, threshold float64, target int) domain.WorkflowLifecyclePolicyRule {
	return domain.WorkflowLifecyclePolicyRule{
		Metric:    metric,
		Threshold: threshold,
		Action:    domain.WorkflowLifecyclePolicyAction{Type: domain.WorkflowPolicyActionSetVersion, TargetVersion: target},
	}
}

func resolve(rolling []domain.WorkflowInvocationSnapshot, rules []domain.WorkflowLifecyclePolicyRule, vm *domain.WorkflowVersionMetrics) (bool, scheduler.WorkflowGoalState) {
	e := scheduler.NewLifecyclePolicyEngine(engineLogger)
	if vm == nil {
		vm = &domain.WorkflowVersionMetrics{}
	}
	policy := domain.WorkflowLifecyclePolicy{Rules: rules}
	updated, gs, err := e.ResolvePolicy(&rolling, &policy, vm)
	if err != nil {
		panic(err)
	}
	return updated, gs
}

func TestResolvePolicy_NoRollingMetrics_NoFire(t *testing.T) {
	updated, _ := resolve(
		nil,
		[]domain.WorkflowLifecyclePolicyRule{cooldownRule(domain.WorkflowMetricNumFailures, 1, 1, 60)},
		nil,
	)
	if updated {
		t.Fatal("expected no rule to fire with empty rolling metrics")
	}
}

func TestResolvePolicy_Cooldown_FiresOnWindowedSum(t *testing.T) {
	rolling := []domain.WorkflowInvocationSnapshot{
		{Failures: ip(1)},
		{Failures: ip(1)},
		{Failures: ip(1)},
	}
	updated, gs := resolve(rolling, []domain.WorkflowLifecyclePolicyRule{
		cooldownRule(domain.WorkflowMetricNumFailures, 3, 3, 90),
	}, nil)

	if !updated {
		t.Fatal("expected cooldown rule to fire (3 failures >= threshold 3)")
	}
	if gs.State != domain.WorkflowStateCooldown {
		t.Errorf("expected state cooldown, got %s", gs.State)
	}
	if gs.Cooldown != 90 {
		t.Errorf("expected cooldown 90, got %d", gs.Cooldown)
	}
	if gs.VersionChange {
		t.Error("cooldown should not be a version change")
	}
}

func TestResolvePolicy_BelowThreshold_NoFire(t *testing.T) {
	rolling := []domain.WorkflowInvocationSnapshot{{Failures: ip(1)}, {Failures: ip(1)}}
	updated, _ := resolve(rolling, []domain.WorkflowLifecyclePolicyRule{
		cooldownRule(domain.WorkflowMetricNumFailures, 5, 2, 60),
	}, nil)
	if updated {
		t.Fatal("expected no fire (sum 2 < threshold 5)")
	}
}

func TestResolvePolicy_MetricNotEmittedInLastRun_Skips(t *testing.T) {
	// Last run did not emit Failures (only cost), so a num_failures rule must be skipped
	// even though earlier runs had failures above threshold.
	rolling := []domain.WorkflowInvocationSnapshot{
		{Failures: ip(10)},
		{CostUsd: fp(1)},
	}
	updated, _ := resolve(rolling, []domain.WorkflowLifecyclePolicyRule{
		cooldownRule(domain.WorkflowMetricNumFailures, 1, 1, 60),
	}, nil)
	if updated {
		t.Fatal("expected skip: num_failures not emitted in latest run")
	}
}

func TestResolvePolicy_InsufficientWindow_Skips(t *testing.T) {
	rolling := []domain.WorkflowInvocationSnapshot{{Failures: ip(9)}} // only 1 observed
	updated, _ := resolve(rolling, []domain.WorkflowLifecyclePolicyRule{
		cooldownRule(domain.WorkflowMetricNumFailures, 1, 3, 60), // needs window 3
	}, nil)
	if updated {
		t.Fatal("expected skip: fewer observed snapshots than window")
	}
}

func TestResolvePolicy_OnlyCountsEmittedSnapshotsInWindow(t *testing.T) {
	// Window of 2 over snapshots where only some emitted the metric: the filter should
	// pick the last 2 *emitting* runs (cost 4 + 5 = 9), ignoring the non-emitting one.
	rolling := []domain.WorkflowInvocationSnapshot{
		{CostUsd: fp(4)},
		{Failures: ip(1)}, // does not emit cost — excluded
		{CostUsd: fp(5)},
	}
	updated, gs := resolve(rolling, []domain.WorkflowLifecyclePolicyRule{
		cooldownRule(domain.WorkflowMetricCost, 9, 2, 30),
	}, nil)
	if !updated {
		t.Fatal("expected fire: cost 4+5=9 >= 9 over the two emitting runs")
	}
	if gs.State != domain.WorkflowStateCooldown {
		t.Errorf("expected cooldown, got %s", gs.State)
	}
}

func TestResolvePolicy_SetVersion_UsesVersionTotals(t *testing.T) {
	// Gate: latest run must emit the metric. Threshold checked against version totals.
	rolling := []domain.WorkflowInvocationSnapshot{{Failures: ip(1)}}
	vm := &domain.WorkflowVersionMetrics{TotalFailures: 10}
	updated, gs := resolve(rolling, []domain.WorkflowLifecyclePolicyRule{
		setVersionRule(domain.WorkflowMetricNumFailures, 10, 7),
	}, vm)

	if !updated {
		t.Fatal("expected set_version to fire (total failures 10 >= 10)")
	}
	if !gs.VersionChange {
		t.Error("expected VersionChange=true")
	}
	if gs.Version != 7 {
		t.Errorf("expected target version 7, got %d", gs.Version)
	}
}

func TestResolvePolicy_PauseBeatsCooldownAndSetVersion(t *testing.T) {
	// All three fire; pause has the highest precedence.
	rolling := []domain.WorkflowInvocationSnapshot{{Failures: ip(5), CostUsd: fp(100), LlmCalls: ip(3)}}
	vm := &domain.WorkflowVersionMetrics{TotalLLMCalls: 3}
	updated, gs := resolve(rolling, []domain.WorkflowLifecyclePolicyRule{
		cooldownRule(domain.WorkflowMetricNumFailures, 1, 1, 30),
		pauseRule(domain.WorkflowMetricCost, 1, 1),
		setVersionRule(domain.WorkflowMetricNumLLMCalls, 1, 9),
	}, vm)

	if !updated {
		t.Fatal("expected a rule to fire")
	}
	if gs.State != domain.WorkflowStatePaused {
		t.Errorf("expected pause to win precedence, got state %s VersionChange=%v", gs.State, gs.VersionChange)
	}
}

func TestResolvePolicy_ToolFailureRate_Cooldown(t *testing.T) {
	rolling := []domain.WorkflowInvocationSnapshot{
		{ToolFailureCounts: map[string]int{"search": 2}},
		{ToolFailureCounts: map[string]int{"search": 3}},
	}
	updated, gs := resolve(rolling, []domain.WorkflowLifecyclePolicyRule{
		{
			Metric:    domain.WorkflowMetricToolFailureRate,
			Threshold: 5,
			Window:    2,
			ToolName:  "search",
			Action:    domain.WorkflowLifecyclePolicyAction{Type: domain.WorkflowPolicyActionCooldown, CooldownSeconds: 45},
		},
	}, nil)
	if !updated || gs.Cooldown != 45 {
		t.Fatalf("expected tool failure cooldown to fire with cooldown 45, got updated=%v cooldown=%d", updated, gs.Cooldown)
	}
}

func TestMetricEmitted(t *testing.T) {
	full := domain.WorkflowInvocationSnapshot{
		CostUsd:            fp(1),
		LlmCalls:           ip(1),
		LatencySeconds:     fp(1),
		Failures:           ip(1),
		ApprovalRejections: ip(1),
		ToolFailureCounts:  map[string]int{"t": 1},
	}
	empty := domain.WorkflowInvocationSnapshot{}

	cases := []struct {
		metric domain.WorkflowMetric
		tool   string
	}{
		{domain.WorkflowMetricCost, ""},
		{domain.WorkflowMetricNumLLMCalls, ""},
		{domain.WorkflowMetricLatency, ""},
		{domain.WorkflowMetricNumFailures, ""},
		{domain.WorkflowMetricApprovalRejections, ""},
		{domain.WorkflowMetricToolFailureRate, "t"},
	}
	for _, c := range cases {
		if !scheduler.MetricEmitted(full, c.metric, c.tool) {
			t.Errorf("expected %s emitted in full snapshot", c.metric)
		}
		if scheduler.MetricEmitted(empty, c.metric, c.tool) {
			t.Errorf("expected %s not emitted in empty snapshot", c.metric)
		}
	}
}
