package metrics

import (
	"context"
	"log/slog"
	"time"

	boundflowv1 "github.com/boundflow/boundflow/gen/boundflow/v1"

	"github.com/boundflow/boundflow/internal/domain"
	"github.com/boundflow/boundflow/internal/storage"
)

type MetricsHandler struct {
	workflow       storage.WorkflowRepository
	agentState     storage.AgentStateRepository
	versionMetrics storage.VersionMetricsRepository
	metrics        storage.MetricsRepository
	log            *slog.Logger
}

func NewMetricsHandler(workflow storage.WorkflowRepository, agentState storage.AgentStateRepository, versionMetrics storage.VersionMetricsRepository, metrics storage.MetricsRepository, log *slog.Logger) *MetricsHandler {
	return &MetricsHandler{
		workflow:       workflow,
		agentState:     agentState,
		versionMetrics: versionMetrics,
		metrics:        metrics,
		log:            log.With("component", "metrics_handler"),
	}
}

func (m *MetricsHandler) HandleAgentMetrics(ctx context.Context, requestID string, invocationMetrics map[string]*boundflowv1.AgentInvocationMetrics, workflowMetrics domain.WorkflowJobMetrics, workflow *domain.Workflow) (error, *domain.WorkflowVersionMetrics) {

	workFlowId := workflow.ID

	versionMetrics, err := m.versionMetrics.GetCurrentVersionMetrics(ctx, workFlowId, workflow.CurrentWorkflowVersion)
	if err != nil {
		m.log.Error("get current version metrics", "workflow_id", workFlowId, "error", err)
		return err, nil
	}
	if versionMetrics == nil {
		versionMetrics = &domain.WorkflowVersionMetrics{
			WorkflowID: workFlowId,
			Version:            workflow.CurrentWorkflowVersion,
			Epoch:              1,
			ToolFailureCounts:  map[string]int{},
		}
	}
	if versionMetrics.ToolFailureCounts == nil {
		versionMetrics.ToolFailureCounts = map[string]int{}
	}

	agentStates, err := m.agentState.GetAllForWorkflow(ctx, workFlowId)
	if err != nil {
		m.log.Error("get agent states", "workflow_id", workFlowId, "error", err)
		return err, nil
	}

	// Build the workflow-level snapshot for this run as the sum across all agents.
	snapshot := domain.WorkflowInvocationSnapshot{RanAt: time.Now().UnixMilli(), RequestID: requestID}
	versionMetrics.RunCount++

	for agent, metrics := range invocationMetrics {
		if metrics == nil {
			continue
		}

		// Append this run's snapshot to the agent's history (metrics only, never policy).
		agentSnapshot := toAgentInvocationSnapshot(metrics, requestID)
		if st, ok := agentStates[agent]; ok {
			st.InvocationMetrics = append(st.InvocationMetrics, agentSnapshot)
		} else {
			agentStates[agent] = &domain.AgentState{
				WorkflowID: workFlowId,
				AgentName:          agent,
				InvocationMetrics:  []domain.AgentInvocationSnapshot{agentSnapshot},
			}
		}

		// Fold into version totals and the workflow snapshot (summed across agents).
		// tokens_used / calls_per_tool are agent-only and don't roll up to the workflow.
		if metrics.CostUsd != nil {
			versionMetrics.TotalCost += *metrics.CostUsd
			m.accF64(&snapshot.CostUsd, *metrics.CostUsd)
		}
		if metrics.LlmCalls != nil {
			versionMetrics.TotalLLMCalls += int(*metrics.LlmCalls)
			m.accInt(&snapshot.LlmCalls, int(*metrics.LlmCalls))
		}
		if metrics.LatencySeconds != nil {
			versionMetrics.TotalLatencySeconds += *metrics.LatencySeconds
			m.accF64(&snapshot.LatencySeconds, *metrics.LatencySeconds)
		}
		if metrics.ApprovalRejections != nil {
			versionMetrics.TotalApprovalRejections += int(*metrics.ApprovalRejections)
			m.accInt(&snapshot.ApprovalRejections, int(*metrics.ApprovalRejections))
		}
		for tool, failCount := range metrics.ToolFailureCounts {
			versionMetrics.ToolFailureCounts[tool] += int(failCount)
			if snapshot.ToolFailureCounts == nil {
				snapshot.ToolFailureCounts = map[string]int{}
			}
			snapshot.ToolFailureCounts[tool] += int(failCount)
		}
	}

	// Fold workflow-level metrics into the run snapshot + version totals.
	// Failures are customer-reported (ctx.MarkFailed); approval rejections are recorded
	// server-side by the rpcworker when a gate is rejected or times out.
	if workflowMetrics.Failures > 0 {
		versionMetrics.TotalFailures += workflowMetrics.Failures
		m.accInt(&snapshot.Failures, workflowMetrics.Failures)
	}
	if workflowMetrics.ApprovalRejections > 0 {
		versionMetrics.TotalApprovalRejections += workflowMetrics.ApprovalRejections
		m.accInt(&snapshot.ApprovalRejections, workflowMetrics.ApprovalRejections)
	}

	// Collect the updated histories for only the agents touched this run.
	agentMetrics := make(map[string][]domain.AgentInvocationSnapshot, len(invocationMetrics))
	for agent := range invocationMetrics {
		agentMetrics[agent] = agentStates[agent].InvocationMetrics
	}

	workflow.InvocationMetrics = append(workflow.InvocationMetrics, snapshot)

	applied, err := m.metrics.EmitMetrics(ctx, workFlowId, workflow.CurrentVersion, workflow.InvocationMetrics, versionMetrics, agentMetrics)
	if err != nil {
		m.log.Error("emit metrics", "workflow_id", workFlowId, "error", err)
		return err, nil
	}
	if !applied {
		m.log.Debug("metrics already emitted for this run, skipping", "workflow_id", workFlowId, "version", workflow.CurrentVersion)
	}
	return nil, versionMetrics
}

// toAgentInvocationSnapshot converts the client-reported wire metrics into the
// stored domain snapshot, stamping requestID -- server-known metadata the SDK
// never sends -- on the way in.
func toAgentInvocationSnapshot(metrics *boundflowv1.AgentInvocationMetrics, requestID string) domain.AgentInvocationSnapshot {
	toIntPtr := func(v *int32) *int {
		if v == nil {
			return nil
		}
		x := int(*v)
		return &x
	}
	var toolFailureCounts, callsPerTool map[string]int
	if len(metrics.ToolFailureCounts) > 0 {
		toolFailureCounts = make(map[string]int, len(metrics.ToolFailureCounts))
		for tool, count := range metrics.ToolFailureCounts {
			toolFailureCounts[tool] = int(count)
		}
	}
	if len(metrics.CallsPerTool) > 0 {
		callsPerTool = make(map[string]int, len(metrics.CallsPerTool))
		for tool, count := range metrics.CallsPerTool {
			callsPerTool[tool] = int(count)
		}
	}
	return domain.AgentInvocationSnapshot{
		CostUsd:            metrics.CostUsd,
		LlmCalls:           toIntPtr(metrics.LlmCalls),
		TokensUsed:         toIntPtr(metrics.TokensUsed),
		LatencySeconds:     metrics.LatencySeconds,
		Failures:           toIntPtr(metrics.Failures),
		ApprovalRejections: toIntPtr(metrics.ApprovalRejections),
		ToolFailureCounts:  toolFailureCounts,
		CallsPerTool:       callsPerTool,
		RanAt:              metrics.RanAt,
		RequestID:          requestID,
	}
}

// accF64 adds v into the domain snapshot field, allocating it on first emit (nil = not emitted).
func (m *MetricsHandler) accF64(dst **float64, v float64) {
	if *dst == nil {
		x := v
		*dst = &x
		return
	}
	**dst += v
}

// accInt adds v into the domain snapshot field, allocating it on first emit (nil = not emitted).
func (m *MetricsHandler) accInt(dst **int, v int) {
	if *dst == nil {
		x := v
		*dst = &x
		return
	}
	**dst += v
}

func (m *MetricsHandler) MergeAgentMetrics(opMetrics map[string]*boundflowv1.AgentInvocationMetrics, jobMetrics *map[string]*boundflowv1.AgentInvocationMetrics) {
	m.log.Debug("merging operation agent metrics into job accumulator", "agents_in_operation", len(opMetrics), "agents_in_job", len(*jobMetrics))
	for agent, opMetric := range opMetrics {
		if opMetric == nil {
			m.log.Debug("skipping nil agent metrics", "agent", agent)
			continue
		}
		jobMetric, ok := (*jobMetrics)[agent]
		if !ok {
			m.log.Debug("carrying agent metrics over fresh", "agent", agent)
			(*jobMetrics)[agent] = opMetric
			continue
		}
		m.log.Debug("summing agent metrics into existing accumulator", "agent", agent)
		// For every metric the operation emitted: sum it into the existing job
		// metric if that field already exists, otherwise carry it over fresh.
		m.addF64(&jobMetric.CostUsd, opMetric.CostUsd)
		m.addI32(&jobMetric.LlmCalls, opMetric.LlmCalls)
		m.addI32(&jobMetric.TokensUsed, opMetric.TokensUsed)
		m.addF64(&jobMetric.LatencySeconds, opMetric.LatencySeconds)
		m.addI32(&jobMetric.Failures, opMetric.Failures)
		m.addI32(&jobMetric.ApprovalRejections, opMetric.ApprovalRejections)
		if len(opMetric.ToolFailureCounts) > 0 {
			if jobMetric.ToolFailureCounts == nil {
				jobMetric.ToolFailureCounts = make(map[string]int32, len(opMetric.ToolFailureCounts))
			}
			for tool, count := range opMetric.ToolFailureCounts {
				jobMetric.ToolFailureCounts[tool] += count
			}
		}
		if len(opMetric.CallsPerTool) > 0 {
			if jobMetric.CallsPerTool == nil {
				jobMetric.CallsPerTool = make(map[string]int32, len(opMetric.CallsPerTool))
			}
			for tool, count := range opMetric.CallsPerTool {
				jobMetric.CallsPerTool[tool] += count
			}
		}
	}
}

// MergeWorkflowMetrics sums the operation's workflow-level metrics into the job accumulator.
func (m *MetricsHandler) MergeWorkflowMetrics(opMetrics domain.WorkflowJobMetrics, jobMetrics *domain.WorkflowJobMetrics) {
	jobMetrics.Failures += opMetrics.Failures
	jobMetrics.ApprovalRejections += opMetrics.ApprovalRejections
}

// addF64 sums src into *dst. If src is nil it's a no-op; if *dst is nil the value is carried over fresh.
func (m *MetricsHandler) addF64(dst **float64, src *float64) {
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
func (m *MetricsHandler) addI32(dst **int32, src *int32) {
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
