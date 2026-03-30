CREATE TABLE scheduler_partitions (
    id                      TEXT PRIMARY KEY,
    resource_instance_count INT NOT NULL DEFAULT 0,
    owner                   TEXT,
    lease_until             TIMESTAMPTZ
);

INSERT INTO scheduler_partitions (id) VALUES
    ('0'), ('1'), ('2'), ('3'), ('4'),
    ('5'), ('6'), ('7'), ('8'), ('9');
