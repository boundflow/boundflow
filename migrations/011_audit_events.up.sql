-- Generic governance audit log. One row per recorded event; the typed payload
-- lives in `details` (JSONB) and is resolved per `event_type` on read (e.g.
-- 'approval' -> approval decision record). Common query dimensions are columns.
CREATE TABLE audit_events (
    id               UUID        PRIMARY KEY,
    tenant_group_id  TEXT        NOT NULL,
    workflow_id      TEXT,
    request_id       TEXT,
    event_type       TEXT        NOT NULL,
    actor            TEXT        NOT NULL DEFAULT '',
    occurred_at      TIMESTAMPTZ NOT NULL,
    details          JSONB       NOT NULL DEFAULT '{}'::jsonb,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_audit_events_tenant_time ON audit_events (tenant_group_id, occurred_at DESC);
CREATE INDEX idx_audit_events_workflow    ON audit_events (workflow_id);
CREATE INDEX idx_audit_events_type        ON audit_events (event_type);
