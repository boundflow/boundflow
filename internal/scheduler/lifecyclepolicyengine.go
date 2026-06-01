package scheduler

import (
	"log/slog"

	"github.com/convergeplane/convergeplane/internal/domain"
)

type LifecyclePolicyEngine struct {
	log              *slog.Logger
	actionPriorities map[domain.WorkflowPolicyActionType]int
}

func NewLifecyclePolicyEngine(log *slog.Logger) *LifecyclePolicyEngine {
	return &LifecyclePolicyEngine{
		log: log,
		actionPriorities: map[domain.WorkflowPolicyActionType]int{
			domain.WorkflowPolicyActionSetVersion: 0,
			domain.WorkflowPolicyActionCooldown:   1,
			domain.WorkflowPolicyActionPause:      2,
		},
	}
}

type WorkflowGoalState struct {
	Version       int
	State         domain.WorkflowState
	Cooldown      int
	VersionChange bool
}

func (e *LifecyclePolicyEngine) ResolvePolicy(rollingMetrics *[]domain.WorkflowInvocationSnapshot, policy *domain.WorkflowLifecyclePolicy, versionMetrics *domain.WorkflowVersionMetrics) (bool, WorkflowGoalState, error) {

	// order of precedence of actiontype:
	// pause, cooldown, setversion

	var workflowGoalState WorkflowGoalState
	winningActionPriority := -1 // lowest precedence

	e.log.Debug("evaluating lifecycle policy", "num_rules", len(policy.Rules))

	for _, rule := range policy.Rules {

		var targetGoalState WorkflowGoalState
		ruleEnforced := false

		if len(*rollingMetrics) == 0 {
			continue
		}

		// Skip if the metric was not emitted in the most recent run.
		if !MetricEmitted((*rollingMetrics)[len(*rollingMetrics)-1], rule.Metric, rule.ToolName) {
			e.log.Debug("metric not emitted in last run, skipping rule", "metric", rule.Metric)
			continue
		}

		switch rule.Action.Type {
		case domain.WorkflowPolicyActionSetVersion:

			var val float64
			switch rule.Metric {
			case domain.WorkflowMetricNumFailures:
				val = float64(versionMetrics.TotalFailures)
			case domain.WorkflowMetricCost:
				val = versionMetrics.TotalCost
			case domain.WorkflowMetricNumLLMCalls:
				val = float64(versionMetrics.TotalLLMCalls)
			case domain.WorkflowMetricLatency:
				val = versionMetrics.TotalLatencySeconds
			case domain.WorkflowMetricApprovalRejections:
				val = float64(versionMetrics.TotalApprovalRejections)
			case domain.WorkflowMetricToolFailureRate:
				intVal, ok := versionMetrics.ToolFailureCounts[rule.ToolName]
				if !ok {
					e.log.Debug("tool not found in version metrics, skipping rule", "tool", rule.ToolName, "metric", rule.Metric)
					continue
				}
				val = float64(intVal)
			}

			e.log.Debug("evaluating set_version rule", "metric", rule.Metric, "val", val, "threshold", rule.Threshold)

			if val >= rule.Threshold {
				ruleEnforced = true
				targetGoalState.Version = rule.Action.TargetVersion
				targetGoalState.VersionChange = true
			}

		case domain.WorkflowPolicyActionCooldown, domain.WorkflowPolicyActionPause:

			// Filter to snapshots where this metric was actually observed.
			var observed []domain.WorkflowInvocationSnapshot
			for _, m := range *rollingMetrics {
				if MetricEmitted(m, rule.Metric, rule.ToolName) {
					observed = append(observed, m)
				}
			}

			if len(observed) < rule.Window {
				e.log.Debug("insufficient observed metrics for rule window, skipping", "have", len(observed), "need", rule.Window, "metric", rule.Metric)
				continue
			}

			lastN := observed[len(observed)-rule.Window:]
			total := 0.0

			for _, metric := range lastN {
				var val float64
				switch rule.Metric {
				case domain.WorkflowMetricNumFailures:
					val = float64(*metric.Failures)
				case domain.WorkflowMetricCost:
					val = *metric.CostUsd
				case domain.WorkflowMetricNumLLMCalls:
					val = float64(*metric.LlmCalls)
				case domain.WorkflowMetricLatency:
					val = *metric.LatencySeconds
				case domain.WorkflowMetricApprovalRejections:
					val = float64(*metric.ApprovalRejections)
				case domain.WorkflowMetricToolFailureRate:
					val = float64(metric.ToolFailureCounts[rule.ToolName])
				}
				total += val
			}

			e.log.Debug("evaluating rolling rule", "action", rule.Action.Type, "metric", rule.Metric, "tool", rule.ToolName, "total", total, "threshold", rule.Threshold, "window", rule.Window)

			if total >= rule.Threshold {
				ruleEnforced = true
				targetGoalState.VersionChange = false
				switch rule.Action.Type {
				case domain.WorkflowPolicyActionCooldown:
					targetGoalState.Cooldown = rule.Action.CooldownSeconds
					targetGoalState.State = domain.WorkflowStateCooldown
				case domain.WorkflowPolicyActionPause:
					targetGoalState.State = domain.WorkflowStatePaused
				}
			}
		}

		if !ruleEnforced {
			e.log.Debug("rule did not fire", "action", rule.Action.Type, "metric", rule.Metric)
			continue
		}

		actionPriority := e.getActionPriority(rule.Action.Type)
		if actionPriority > winningActionPriority {
			winningActionPriority = actionPriority
			workflowGoalState = targetGoalState
		}
	}

	if winningActionPriority == -1 {
		e.log.Debug("no rules fired, no state change")
	} else {
		e.log.Info("lifecycle policy fired", "action", workflowGoalState.State, "version_change", workflowGoalState.VersionChange, "target_version", workflowGoalState.Version, "cooldown_seconds", workflowGoalState.Cooldown)
	}

	return (winningActionPriority != -1), workflowGoalState, nil
}

func (e *LifecyclePolicyEngine) getActionPriority(actionType domain.WorkflowPolicyActionType) int {
	return e.actionPriorities[actionType]
}

// metricEmitted reports whether the given metric was observed in the snapshot.
// A nil scalar field (or absent tool key) means the metric was not emitted that run.
func MetricEmitted(snapshot domain.WorkflowInvocationSnapshot, metric domain.WorkflowMetric, toolName string) bool {
	switch metric {
	case domain.WorkflowMetricNumFailures:
		return snapshot.Failures != nil
	case domain.WorkflowMetricCost:
		return snapshot.CostUsd != nil
	case domain.WorkflowMetricNumLLMCalls:
		return snapshot.LlmCalls != nil
	case domain.WorkflowMetricLatency:
		return snapshot.LatencySeconds != nil
	case domain.WorkflowMetricApprovalRejections:
		return snapshot.ApprovalRejections != nil
	case domain.WorkflowMetricToolFailureRate:
		_, ok := snapshot.ToolFailureCounts[toolName]
		return ok
	}
	return false
}
