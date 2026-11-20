-- +goose Up
CREATE INDEX idx_job_instance_history_expired ON job_instance (finished_at, id)
    WHERE status IN (3, 4, 5) AND finished_at IS NOT NULL;

COMMENT ON INDEX idx_job_instance_history_expired IS 'Keyset traversal of terminal execution history by finish time';

-- +goose Down
DROP INDEX idx_job_instance_history_expired;
