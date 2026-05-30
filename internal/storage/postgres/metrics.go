package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	convergeplanev1 "github.com/convergeplane/convergeplane/gen/convergeplane/v1"
	"github.com/convergeplane/convergeplane/internal/domain"
)

type MetricsRepo struct {
	pool *pgxpool.Pool
}

func NewMetricsRepo(pool *pgxpool.Pool) *MetricsRepo {
	return &MetricsRepo{pool: pool}
}

// EmitMetrics atomically (a) appends the run's rolling snapshot to the resource's
// invocation_metrics, (b) upserts the version-metric totals, and (c) upserts each agent's
// invocation-metrics history (metrics only — never policy) — but only if the resource's
// metrics_emitted_at is still strictly less than emittedVersion. The gate makes the write
// idempotent per run: a retry for an already-emitted version is a no-op.
// Returns false (no error) when the gate fails.
func (r *MetricsRepo) EmitMetrics(
	ctx context.Context,
	resourceInstanceID string,
	emittedVersion int64,
	rollingMetrics []domain.WorkflowInvocationSnapshot,
	versionMetrics *domain.WorkflowVersionMetrics,
	agentMetrics map[string][]*convergeplanev1.AgentInvocationMetrics,
) (bool, error) {
	rollingJSON, err := json.Marshal(rollingMetrics)
	if err != nil {
		return false, fmt.Errorf("marshal rolling metrics: %w", err)
	}
	toolFailureJSON, err := json.Marshal(versionMetrics.ToolFailureCounts)
	if err != nil {
		return false, fmt.Errorf("marshal tool failure counts: %w", err)
	}

	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("begin emit metrics tx: %w", err)
	}
	defer tx.Rollback(ctx)

	// Gate + advance metrics_emitted_at + append the rolling snapshot, all conditional on the gate.
	var gatedID string
	err = tx.QueryRow(ctx, `
		UPDATE resource_instances
		SET metrics_emitted_at = $2,
		    invocation_metrics = $3::jsonb
		WHERE id = $1 AND metrics_emitted_at < $2
		RETURNING id
	`, resourceInstanceID, emittedVersion, rollingJSON).Scan(&gatedID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, nil // already emitted for this run
		}
		return false, fmt.Errorf("gate + append rolling metrics: %w", err)
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO workflow_version_metrics
		    (resource_instance_id, version, epoch, total_cost, run_count, total_failures,
		     total_llm_calls, total_latency_seconds, total_approval_rejections, tool_failure_counts)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
		ON CONFLICT (resource_instance_id, version, epoch) DO UPDATE SET
		    total_cost                = EXCLUDED.total_cost,
		    run_count                 = EXCLUDED.run_count,
		    total_failures            = EXCLUDED.total_failures,
		    total_llm_calls           = EXCLUDED.total_llm_calls,
		    total_latency_seconds     = EXCLUDED.total_latency_seconds,
		    total_approval_rejections = EXCLUDED.total_approval_rejections,
		    tool_failure_counts       = EXCLUDED.tool_failure_counts
	`, resourceInstanceID, versionMetrics.Version, versionMetrics.Epoch,
		versionMetrics.TotalCost, versionMetrics.RunCount, versionMetrics.TotalFailures,
		versionMetrics.TotalLLMCalls, versionMetrics.TotalLatencySeconds,
		versionMetrics.TotalApprovalRejections, toolFailureJSON,
	)
	if err != nil {
		return false, fmt.Errorf("upsert version metrics: %w", err)
	}

	for agent, history := range agentMetrics {
		historyJSON, err := json.Marshal(history)
		if err != nil {
			return false, fmt.Errorf("marshal agent metrics for %s: %w", agent, err)
		}
		_, err = tx.Exec(ctx, `
			INSERT INTO agent_state (resource_instance_id, agent_name, invocation_metrics)
			VALUES ($1, $2, $3)
			ON CONFLICT (resource_instance_id, agent_name)
			DO UPDATE SET invocation_metrics = $3, updated_at = now()
		`, resourceInstanceID, agent, historyJSON)
		if err != nil {
			return false, fmt.Errorf("upsert agent metrics for %s: %w", agent, err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("commit emit metrics tx: %w", err)
	}
	return true, nil
}
