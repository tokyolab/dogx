-- +goose Up
-- Group deletion checks retained execution dependencies only after confirming
-- that no undeleted Job refers to the group. Terminal history must not require
-- a full Instance scan for this low-frequency management operation.
CREATE INDEX idx_job_instance_active_group
 ON job_instance (executor_group_id) WHERE status IN (1, 2, 6);

-- +goose Down
DROP INDEX idx_job_instance_active_group;
