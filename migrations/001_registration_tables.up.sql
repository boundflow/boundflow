CREATE TABLE tenant_groups (
    id          TEXT PRIMARY KEY,
    name        TEXT NOT NULL,
    policies    JSONB NOT NULL DEFAULT '{}',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE tenants (
    id              TEXT NOT NULL,
    tenant_group_id TEXT NOT NULL REFERENCES tenant_groups(id),
    name            TEXT NOT NULL,
    policy_overrides JSONB,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_group_id, id)
);

CREATE TABLE resources (
    id            TEXT NOT NULL,
    tenant_id     TEXT NOT NULL,
    resource_type TEXT NOT NULL,
    current_state JSONB NOT NULL DEFAULT '{}',
    goal_state    JSONB NOT NULL DEFAULT '{}',
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, id)
);
