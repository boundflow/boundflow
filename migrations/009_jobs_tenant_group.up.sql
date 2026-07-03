ALTER TABLE jobs ADD COLUMN tenant_group_id TEXT NOT NULL DEFAULT '';
CREATE INDEX jobs_tenant_group_status_idx ON jobs (tenant_group_id, status);
