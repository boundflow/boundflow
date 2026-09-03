-- 5e39cdc ("feat: soft-delete tenants/workflows and reconcile purges
-- asynchronously") added tenants.deleted_at/workflow_count/scheduler_partition_id
-- and workflows.deletion_finalized_at by editing 001_registration_tables.up.sql
-- and 002_workflow_table.up.sql in place, on the assumption that no self-hosted
-- deployment had data yet. That's true of a fresh install, which runs the
-- corrected 001/002 and already has these columns — but any database migrated
-- before that commit shipped (v0.1.0-v0.4.0) ran the original 001/002 and, since
-- golang-migrate tracks applied migrations by version number rather than
-- content, will never re-run them. Upgrading such a database to a later image
-- then fails wherever this code assumes the columns exist (e.g. ListTenants:
-- "column deleted_at does not exist").
--
-- This backfills them. It's a genuine backfill on a pre-5e39cdc database and a
-- no-op on a fresh one (IF NOT EXISTS throughout), so it's safe either way.

ALTER TABLE tenants ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMPTZ;
ALTER TABLE tenants ADD COLUMN IF NOT EXISTS workflow_count INTEGER NOT NULL DEFAULT 0;
ALTER TABLE tenants ADD COLUMN IF NOT EXISTS scheduler_partition_id TEXT;
CREATE INDEX IF NOT EXISTS idx_tenants_purgeable ON tenants (scheduler_partition_id) WHERE deleted_at IS NOT NULL;

ALTER TABLE workflows ADD COLUMN IF NOT EXISTS deletion_finalized_at TIMESTAMPTZ;
CREATE INDEX IF NOT EXISTS idx_workflows_purgeable ON workflows (scheduler_partition_id, deletion_finalized_at) WHERE lifecycle_state = 'deleted';
