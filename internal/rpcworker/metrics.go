package rpcworker

import (
	"context"
	"log/slog"
	"time"

	convergeplanev1 "github.com/convergeplane/convergeplane/gen/convergeplane/v1"

	"github.com/convergeplane/convergeplane/internal/domain"
	"github.com/convergeplane/convergeplane/internal/storage"
)

type MetricsHandler struct {
	workflow       storage.ResourceInstanceRepository
	agentState     storage.AgentStateRepository
	versionMetrics storage.VersionMetricsRepository
	metrics        storage.MetricsRepository
	log            *slog.Logger
}

func (m *MetricsHandler) HandleAgentMetrics(ctx context.Context, invocationMetrics map[string]*convergeplanev1.AgentInvocationMetrics, workFlowId string) error {
	workflow, err := m.workflow.Get(ctx, workFlowId)
	if err != nil {
		m.log.Error("get resource instance", "resource_id", workFlowId, "error", err)
		return err
	}

	versionMetrics, err := m.versionMetrics.GetCurrentVersionMetrics(ctx, workFlowId, workflow.CurrentWorkflowVersion)
	if err != nil {
		m.log.Error("get current version metrics", "resource_id", workFlowId, "error", err)
		return err
	}
	if versionMetrics == nil {
		versionMetrics = &domain.WorkflowVersionMetrics{
			ResourceInstanceID: workFlowId,
			Version:            workflow.CurrentWorkflowVersion,
			Epoch:              1,
			ToolFailureCounts:  map[string]int{},
		}
	}
	if versionMetrics.ToolFailureCounts == nil {
		versionMetrics.ToolFailureCounts = map[string]int{}
	}

	agentStates, err := m.agentState.GetAllForResource(ctx, workFlowId)
	if err != nil {
		m.log.Error("get agent states", "resource_id", workFlowId, "error", err)
		return err
	}

	// Build the workflow-level snapshot for this run as the sum across all agents.
	snapshot := domain.WorkflowInvocationSnapshot{RanAt: time.Now().UnixMilli()}
	versionMetrics.RunCount++

	for agent, metrics := range invocationMetrics {
		if metrics == nil {
			continue
		}

		// Append this run's snapshot to the agent's history (metrics only, never policy).
		if st, ok := agentStates[agent]; ok {
			st.InvocationMetrics = append(st.InvocationMetrics, metrics)
		} else {
			agentStates[agent] = &domain.AgentState{
				ResourceInstanceID: workFlowId,
				AgentName:          agent,
				InvocationMetrics:  []*convergeplanev1.AgentInvocationMetrics{metrics},
			}
		}

		// Fold into version totals and the workflow snapshot (summed across agents).
		// tokens_used / calls_per_tool are agent-only and don't roll up to the workflow.
		if metrics.CostUsd != nil {
			versionMetrics.TotalCost += *metrics.CostUsd
			accF64(&snapshot.CostUsd, *metrics.CostUsd)
		}
		if metrics.LlmCalls != nil {
			versionMetrics.TotalLLMCalls += int(*metrics.LlmCalls)
			accInt(&snapshot.LlmCalls, int(*metrics.LlmCalls))
		}
		if metrics.LatencySeconds != nil {
			versionMetrics.TotalLatencySeconds += *metrics.LatencySeconds
			accF64(&snapshot.LatencySeconds, *metrics.LatencySeconds)
		}
		if metrics.Failures != nil {
			versionMetrics.TotalFailures += int(*metrics.Failures)
			accInt(&snapshot.Failures, int(*metrics.Failures))
		}
		if metrics.ApprovalRejections != nil {
			versionMetrics.TotalApprovalRejections += int(*metrics.ApprovalRejections)
			accInt(&snapshot.ApprovalRejections, int(*metrics.ApprovalRejections))
		}
		for tool, failCount := range metrics.ToolFailureCounts {
			versionMetrics.ToolFailureCounts[tool] += int(failCount)
			if snapshot.ToolFailureCounts == nil {
				snapshot.ToolFailureCounts = map[string]int{}
			}
			snapshot.ToolFailureCounts[tool] += int(failCount)
		}
	}

	// Collect the updated histories for only the agents touched this run.
	agentMetrics := make(map[string][]*convergeplanev1.AgentInvocationMetrics, len(invocationMetrics))
	for agent := range invocationMetrics {
		agentMetrics[agent] = agentStates[agent].InvocationMetrics
	}

	applied, err := m.metrics.EmitMetrics(ctx, workFlowId, workflow.CurrentVersion, snapshot, versionMetrics, agentMetrics)
	if err != nil {
		m.log.Error("emit metrics", "resource_id", workFlowId, "error", err)
		return err
	}
	if !applied {
		m.log.Debug("metrics already emitted for this run, skipping", "resource_id", workFlowId, "version", workflow.CurrentVersion)
	}
	return nil
}

// accF64 adds v into the domain snapshot field, allocating it on first emit (nil = not emitted).
func accF64(dst **float64, v float64) {
	if *dst == nil {
		x := v
		*dst = &x
		return
	}
	**dst += v
}

// accInt adds v into the domain snapshot field, allocating it on first emit (nil = not emitted).
func accInt(dst **int, v int) {
	if *dst == nil {
		x := v
		*dst = &x
		return
	}
	**dst += v
}

func MergeAgentMetrics(log *slog.Logger, opMetrics map[string]*convergeplanev1.AgentInvocationMetrics, jobMetrics *map[string]*convergeplanev1.AgentInvocationMetrics) {
	log.Debug("merging operation agent metrics into job accumulator", "agents_in_operation", len(opMetrics), "agents_in_job", len(*jobMetrics))
	for agent, opMetric := range opMetrics {
		if opMetric == nil {
			log.Debug("skipping nil agent metrics", "agent", agent)
			continue
		}
		jobMetric, ok := (*jobMetrics)[agent]
		if !ok {
			log.Debug("carrying agent metrics over fresh", "agent", agent)
			(*jobMetrics)[agent] = opMetric
			continue
		}
		log.Debug("summing agent metrics into existing accumulator", "agent", agent)
		// For every metric the operation emitted: sum it into the existing job
		// metric if that field already exists, otherwise carry it over fresh.
		addF64(&jobMetric.CostUsd, opMetric.CostUsd)
		addI32(&jobMetric.LlmCalls, opMetric.LlmCalls)
		addI32(&jobMetric.TokensUsed, opMetric.TokensUsed)
		addI32(&jobMetric.CallsPerTool, opMetric.CallsPerTool)
		addF64(&jobMetric.LatencySeconds, opMetric.LatencySeconds)
		addI32(&jobMetric.Failures, opMetric.Failures)
		addI32(&jobMetric.ApprovalRejections, opMetric.ApprovalRejections)
		if len(opMetric.ToolFailureCounts) > 0 {
			if jobMetric.ToolFailureCounts == nil {
				jobMetric.ToolFailureCounts = make(map[string]int32, len(opMetric.ToolFailureCounts))
			}
			for tool, count := range opMetric.ToolFailureCounts {
				jobMetric.ToolFailureCounts[tool] += count
			}
		}
	}
}

// addF64 sums src into *dst. If src is nil it's a no-op; if *dst is nil the value is carried over fresh.
func addF64(dst **float64, src *float64) {
	if src == nil {
		return
	}
	if *dst == nil {
		v := *src
		*dst = &v
		return
	}
	v := **dst + *src
	*dst = &v
}

// addI32 sums src into *dst. If src is nil it's a no-op; if *dst is nil the value is carried over fresh.
func addI32(dst **int32, src *int32) {
	if src == nil {
		return
	}
	if *dst == nil {
		v := *src
		*dst = &v
		return
	}
	v := **dst + *src
	*dst = &v
}
