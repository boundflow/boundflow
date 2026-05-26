package scheduler

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/convergeplane/convergeplane/internal/domain"
	"github.com/convergeplane/convergeplane/internal/storage"
)

type LifecyclePolicyEngine struct {
	log *slog.Logger
}

func NewLifecyclePolicyEngine(log *slog.Logger) *LifecyclePolicyEngine {
	return &LifecyclePolicyEngine {
		log: log,
	}
}

type WorkflowGoalState struct {
	version int
	state domain.WorkflowState
	cooldown int
}

func (e *LifecyclePolicyEngine) ResolvePolicy(rollingMetrics *[]domain.WorkflowInvocationSnapshot, policy *domain.WorkflowLifecyclePolicy, versionMetrics *domain.WorkflowVersionMetrics) (bool, WorkflowGoalState, error){

	var targetVersion int
	policyEnforced := false
	var cooldownPeriod int
	var targetState domain.WorkflowState

	for _, rule := range policy.Rules {

		switch rule.Action.Type {
		case WorkflowPolicyActionSetVersion:
			var val int
			switch rule.Metric {
			case WorkflowMetricNumFailures:
				val = versionMetrics.TotalFailures
			case WorkflowMetricCost:
				val = versionMetrics.TotalCost
			case WorkflowMetricNumLLMCalls:
				val = versionMetrics.TotalLLMCalls
			case WorkflowMetricLatency:
				val = versionMetrics.TotalLatencySeconds
			case WorkflowMetricApprovalRejections:
				val = versionMetrics.TotalApprovalRejections
			case WorkflowMetricToolFailureRate:
				val = versionMetrics.TotalApprovalRejections
			}
		
			if (val >= rule.Threshold) {
				policyEnforced = true
				targetVersion = rule.Action.TargetVersion
			}

		default:

			if len(rollingMetrics) < rule.Window:
				// log error and move on?
				continue

			lastN := rollingMetrics[len(rollingMetrics)-rule.Window:]
			total := 0

			for _, metric := range lastN {
				var val int
				switch rule.Metric {
				case WorkflowMetricNumFailures:
					val = metric.Failures
				case WorkflowMetricCost:
					val = metric.CostUsd
				case WorkflowMetricNumLLMCalls:
					val = metric.LlmCalls
				case WorkflowMetricLatency:
					val = metric.LatencySeconds
				case WorkflowMetricApprovalRejections:
					val = metric.ApprovalRejections
				}
				total += val
			}
			
			if (val >= rule.Threshold) {
				policyEnforced = true
				if (rule.Action.Type == WorkflowPolicyActionCooldown) {
					cooldownPeriod = rule.Action.CooldownSeconds
				}
			}

		}

	}

}