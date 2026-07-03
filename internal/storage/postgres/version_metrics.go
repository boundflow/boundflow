package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/boundflow/boundflow/internal/domain"
)

type VersionMetricsRepo struct {
	pool *pgxpool.Pool
}

func NewVersionMetricsRepo(pool *pgxpool.Pool) *VersionMetricsRepo {
	return &VersionMetricsRepo{pool: pool}
}

func (r *VersionMetricsRepo) GetCurrentVersionMetrics(ctx context.Context, workflowID string, version int) (*domain.WorkflowVersionMetrics, error) {
	var m domain.WorkflowVersionMetrics
	var toolFailureCountsRaw []byte

	err := r.pool.QueryRow(ctx, `
		SELECT workflow_id, version, epoch,
		       total_cost, run_count, total_failures, total_llm_calls,
		       total_latency_seconds, total_approval_rejections, tool_failure_counts
		FROM workflow_version_metrics
		WHERE workflow_id = $1 AND version = $2
		ORDER BY epoch DESC
		LIMIT 1
	`, workflowID, version).Scan(
		&m.WorkflowID, &m.Version, &m.Epoch,
		&m.TotalCost, &m.RunCount, &m.TotalFailures, &m.TotalLLMCalls,
		&m.TotalLatencySeconds, &m.TotalApprovalRejections, &toolFailureCountsRaw,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("query current version metrics: %w", err)
	}

	if err := json.Unmarshal(toolFailureCountsRaw, &m.ToolFailureCounts); err != nil {
		return nil, fmt.Errorf("unmarshal tool_failure_counts: %w", err)
	}

	return &m, nil
}
