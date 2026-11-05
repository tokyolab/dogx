-- +goose Up
-- Management pagination includes every Task type and terminal state. The
-- unfinished-Task partial index cannot serve complete execution history reads.
CREATE INDEX idx_job_task_instance ON job_task (instance_id, id);

-- +goose Down
DROP INDEX idx_job_task_instance;
