-- +goose Up
ALTER TABLE job_task ADD COLUMN input_data JSONB;
COMMENT ON COLUMN job_task.input_data IS 'Fixed shard JSON input or Post statistics retained across attempts; SQL NULL means absent and JSON null is a valid shard value';

DROP INDEX idx_job_task_retry_due;
CREATE INDEX idx_job_task_dispatch_due
 ON job_task (next_dispatch_at, id) WHERE status IN (1, 3);

-- +goose Down
DROP INDEX idx_job_task_dispatch_due;
CREATE INDEX idx_job_task_retry_due
 ON job_task (next_dispatch_at, id) WHERE status = 3;
ALTER TABLE job_task DROP COLUMN input_data;
