-- +goose Up
CREATE INDEX idx_job_instance_history_created ON job_instance (created_at DESC, id DESC);
CREATE INDEX idx_job_instance_history_job ON job_instance (job_id, created_at DESC, id DESC);
CREATE INDEX idx_job_instance_history_group ON job_instance (executor_group_id, created_at DESC, id DESC);

-- +goose Down
DROP INDEX idx_job_instance_history_group;
DROP INDEX idx_job_instance_history_job;
DROP INDEX idx_job_instance_history_created;
