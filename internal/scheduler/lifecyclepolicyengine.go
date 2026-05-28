package scheduler

import (
	"log/slog"
	"slices"

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
	version       int
	state         domain.WorkflowState
	cooldown      int
	versionChange bool
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

		switch rule.Action.Type {
		case domain.WorkflowPolicyActionSetVersion:

			if !slices.Contains(versionMetrics.EmittedMetrics, rule.Metric) {
				e.log.Debug("metric not emitted in last run, skipping set_version rule", "metric", rule.Metric)
				continue
			}

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
				targetGoalState.version = rule.Action.TargetVersion
				targetGoalState.versionChange = true
			}

		case domain.WorkflowPolicyActionCooldown, domain.WorkflowPolicyActionPause:

			if len(*rollingMetrics) == 0 {
				continue
			}

			// Skip if the metric was not emitted in the most recent run.
			last := (*rollingMetrics)[len(*rollingMetrics)-1]
			lastEmitted := false
			switch rule.Metric {
			case domain.WorkflowMetricNumFailures:
				lastEmitted = last.Failures != nil
			case domain.WorkflowMetricCost:
				lastEmitted = last.CostUsd != nil
			case domain.WorkflowMetricNumLLMCalls:
				lastEmitted = last.LlmCalls != nil
			case domain.WorkflowMetricLatency:
				lastEmitted = last.LatencySeconds != nil
			case domain.WorkflowMetricApprovalRejections:
				lastEmitted = last.ApprovalRejections != nil
			case domain.WorkflowMetricToolFailureRate:
				_, lastEmitted = last.ToolFailureCounts[rule.ToolName]
			}
			if !lastEmitted {
				e.log.Debug("metric not emitted in last run, skipping rolling rule", "metric", rule.Metric)
				continue
			}

			// Filter to snapshots where this metric was actually observed.
			var observed []domain.WorkflowInvocationSnapshot
			for _, m := range *rollingMetrics {
				switch rule.Metric {
				case domain.WorkflowMetricNumFailures:
					if m.Failures != nil {
						observed = append(observed, m)
					}
				case domain.WorkflowMetricCost:
					if m.CostUsd != nil {
						observed = append(observed, m)
					}
				case domain.WorkflowMetricNumLLMCalls:
					if m.LlmCalls != nil {
						observed = append(observed, m)
					}
				case domain.WorkflowMetricLatency:
					if m.LatencySeconds != nil {
						observed = append(observed, m)
					}
				case domain.WorkflowMetricApprovalRejections:
					if m.ApprovalRejections != nil {
						observed = append(observed, m)
					}
				case domain.WorkflowMetricToolFailureRate:
					if _, ok := m.ToolFailureCounts[rule.ToolName]; ok {
						observed = append(observed, m)
					}
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
				targetGoalState.versionChange = false
				switch rule.Action.Type {
				case domain.WorkflowPolicyActionCooldown:
					targetGoalState.cooldown = rule.Action.CooldownSeconds
					targetGoalState.state = domain.WorkflowStateCooldown
				case domain.WorkflowPolicyActionPause:
					targetGoalState.state = domain.WorkflowStatePaused
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
		e.log.Info("lifecycle policy fired", "action", workflowGoalState.state, "version_change", workflowGoalState.versionChange, "target_version", workflowGoalState.version, "cooldown_seconds", workflowGoalState.cooldown)
	}

	return (winningActionPriority != -1), workflowGoalState, nil
}

func (e *LifecyclePolicyEngine) getActionPriority(actionType domain.WorkflowPolicyActionType) int {
	return e.actionPriorities[actionType]
}
