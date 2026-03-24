CREATE TABLE tenant_groups (
    id          TEXT PRIMARY KEY,
    name        TEXT NOT NULL,
    policies    JSONB NOT NULL DEFAULT '{}',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO tenant_groups (id, name) VALUES ('default', 'Default');

CREATE TABLE tenants (
    id               TEXT PRIMARY KEY,
    tenant_group_id  TEXT NOT NULL DEFAULT 'default' REFERENCES tenant_groups(id),
    name             TEXT NOT NULL,
    policy_overrides JSONB,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);
