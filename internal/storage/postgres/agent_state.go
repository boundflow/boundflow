package postgres

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"

	boundflowv1 "github.com/boundflow/boundflow/gen/boundflow/v1"
	"github.com/boundflow/boundflow/internal/domain"
)

type AgentStateRepo struct {
	pool *pgxpool.Pool
}

func NewAgentStateRepo(pool *pgxpool.Pool) *AgentStateRepo {
	return &AgentStateRepo{pool: pool}
}

func (r *AgentStateRepo) UpsertRuntimePolicy(ctx context.Context, resourceInstanceID, agentName string, policy map[string]any) error {
	policyJSON, err := json.Marshal(policy)
	if err != nil {
		return fmt.Errorf("marshal runtime policy: %w", err)
	}
	_, err = r.pool.Exec(ctx,
		`INSERT INTO agent_state (resource_instance_id, agent_name, runtime_policy)
		 VALUES ($1, $2, $3)
		 ON CONFLICT (resource_instance_id, agent_name)
		 DO UPDATE SET runtime_policy = $3, updated_at = now()`,
		resourceInstanceID, agentName, policyJSON,
	)
	return err
}

func (r *AgentStateRepo) UpsertLifecyclePolicy(ctx context.Context, resourceInstanceID, agentName string, policy map[string]any) error {
	policyJSON, err := json.Marshal(policy)
	if err != nil {
		return fmt.Errorf("marshal lifecycle policy: %w", err)
	}
	_, err = r.pool.Exec(ctx,
		`INSERT INTO agent_state (resource_instance_id, agent_name, lifecycle_policy)
		 VALUES ($1, $2, $3)
		 ON CONFLICT (resource_instance_id, agent_name)
		 DO UPDATE SET lifecycle_policy = $3, updated_at = now()`,
		resourceInstanceID, agentName, policyJSON,
	)
	return err
}

func (r *AgentStateRepo) UpdateMetrics(ctx context.Context, resourceInstanceID, agentName string, metrics []*boundflowv1.AgentInvocationMetrics) error {
	metricsJSON, err := json.Marshal(metrics)
	if err != nil {
		return fmt.Errorf("marshal metrics: %w", err)
	}
	_, err = r.pool.Exec(ctx,
		`INSERT INTO agent_state (resource_instance_id, agent_name, invocation_metrics)
		 VALUES ($1, $2, $3)
		 ON CONFLICT (resource_instance_id, agent_name)
		 DO UPDATE SET invocation_metrics = $3, updated_at = now()`,
		resourceInstanceID, agentName, metricsJSON,
	)
	return err
}

func (r *AgentStateRepo) GetAllForResource(ctx context.Context, resourceInstanceID string) (map[string]*domain.AgentState, error) {
	rows, err := r.pool.Query(ctx,
		`SELECT agent_name, runtime_policy, lifecycle_policy, invocation_metrics, updated_at
		 FROM agent_state WHERE resource_instance_id = $1`,
		resourceInstanceID,
	)
	if err != nil {
		return nil, fmt.Errorf("get agent states: %w", err)
	}
	defer rows.Close()

	states := make(map[string]*domain.AgentState)
	for rows.Next() {
		var s domain.AgentState
		var runtimeJSON, lifecycleJSON, metricsJSON []byte
		if err := rows.Scan(&s.AgentName, &runtimeJSON, &lifecycleJSON, &metricsJSON, &s.UpdatedAt); err != nil {
			return nil, fmt.Errorf("scan agent state: %w", err)
		}
		s.ResourceInstanceID = resourceInstanceID
		if err := json.Unmarshal(runtimeJSON, &s.RuntimePolicy); err != nil {
			return nil, fmt.Errorf("unmarshal runtime policy: %w", err)
		}
		if err := json.Unmarshal(lifecycleJSON, &s.LifecyclePolicy); err != nil {
			return nil, fmt.Errorf("unmarshal lifecycle policy: %w", err)
		}
		if err := json.Unmarshal(metricsJSON, &s.InvocationMetrics); err != nil {
			return nil, fmt.Errorf("unmarshal invocation metrics: %w", err)
		}
		states[s.AgentName] = &s
	}
	return states, rows.Err()
}

func (r *AgentStateRepo) Delete(ctx context.Context, resourceInstanceID, agentName string) error {
	_, err := r.pool.Exec(ctx,
		`DELETE FROM agent_state WHERE resource_instance_id = $1 AND agent_name = $2`,
		resourceInstanceID, agentName,
	)
	return err
}
