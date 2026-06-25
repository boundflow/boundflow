-- Global default model rates (per 1M tokens, USD). Operator-managed, not
-- customer-editable. Seeded below from Anthropic list prices (2026-06). The
-- effective rate for a tenant is its override if present, else the default here.
CREATE TABLE default_model_pricing (
    model_id      text             NOT NULL PRIMARY KEY,
    input_per_1m  double precision NOT NULL,
    output_per_1m double precision NOT NULL,
    updated_at    timestamptz      NOT NULL DEFAULT now()
);

INSERT INTO default_model_pricing (model_id, input_per_1m, output_per_1m) VALUES
    ('claude-opus-4-8',   5.0, 25.0),
    ('claude-sonnet-4-6', 3.0, 15.0),
    ('claude-haiku-4-5',  1.0,  5.0);

-- Per-tenant-group overrides. The server merges these over the defaults above to
-- produce the effective pricing it ships to each worker at invoke time. Rows here
-- exist only where a customer has overridden a model's rate.
CREATE TABLE model_pricing (
    tenant_group_id text             NOT NULL,
    model_id        text             NOT NULL,
    input_per_1m    double precision NOT NULL,
    output_per_1m   double precision NOT NULL,
    updated_at      timestamptz      NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_group_id, model_id)
);
