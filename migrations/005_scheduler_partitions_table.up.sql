CREATE TABLE scheduler_partitions (
    id                      TEXT PRIMARY KEY,
    workflow_count INT NOT NULL DEFAULT 0,
    owner                   TEXT,
    lease_until             TIMESTAMPTZ
);
