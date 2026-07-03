package postgres

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/boundflow/boundflow/internal/domain"
)

type ModelPricingRepo struct {
	pool *pgxpool.Pool
}

func NewModelPricingRepo(pool *pgxpool.Pool) *ModelPricingRepo {
	return &ModelPricingRepo{pool: pool}
}

func (r *ModelPricingRepo) ListDefaults(ctx context.Context) ([]domain.ModelPricing, error) {
	rows, err := r.pool.Query(ctx,
		`SELECT model_id, input_per_1m, output_per_1m FROM default_model_pricing ORDER BY model_id`,
	)
	if err != nil {
		return nil, handleError(err, "default model pricing")
	}
	defer rows.Close()

	var out []domain.ModelPricing
	for rows.Next() {
		var p domain.ModelPricing
		if err := rows.Scan(&p.ModelID, &p.InputPer1M, &p.OutputPer1M); err != nil {
			return nil, handleError(err, "default model pricing")
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

func (r *ModelPricingRepo) Upsert(ctx context.Context, tenantGroupID string, p domain.ModelPricing) error {
	_, err := r.pool.Exec(ctx,
		`INSERT INTO model_pricing (tenant_group_id, model_id, input_per_1m, output_per_1m, updated_at)
		 VALUES ($1, $2, $3, $4, now())
		 ON CONFLICT (tenant_group_id, model_id)
		 DO UPDATE SET input_per_1m = EXCLUDED.input_per_1m,
		               output_per_1m = EXCLUDED.output_per_1m,
		               updated_at = now()`,
		tenantGroupID, p.ModelID, p.InputPer1M, p.OutputPer1M,
	)
	if err != nil {
		return handleError(err, "model pricing")
	}
	return nil
}

func (r *ModelPricingRepo) ListForTenantGroup(ctx context.Context, tenantGroupID string) ([]domain.ModelPricing, error) {
	rows, err := r.pool.Query(ctx,
		`SELECT model_id, input_per_1m, output_per_1m
		 FROM model_pricing
		 WHERE tenant_group_id = $1
		 ORDER BY model_id`,
		tenantGroupID,
	)
	if err != nil {
		return nil, handleError(err, "model pricing")
	}
	defer rows.Close()

	var out []domain.ModelPricing
	for rows.Next() {
		var p domain.ModelPricing
		if err := rows.Scan(&p.ModelID, &p.InputPer1M, &p.OutputPer1M); err != nil {
			return nil, handleError(err, "model pricing")
		}
		out = append(out, p)
	}
	return out, rows.Err()
}
