-- Next() can delay dispatch of the next operation. NULL means dispatch as soon as
-- eligible (today's behavior); set means the job isn't picked up until this time.
ALTER TABLE jobs ADD COLUMN dispatch_at TIMESTAMPTZ;
