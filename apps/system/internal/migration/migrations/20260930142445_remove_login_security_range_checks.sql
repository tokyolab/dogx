-- +goose Up
-- Configurable business limits are validated by the API and RPC, not the schema.
ALTER TABLE sys_security_config
    DROP CONSTRAINT sys_security_config_login_rate_limit_window_seconds_check,
    DROP CONSTRAINT sys_security_config_login_rate_limit_max_requests_check,
    DROP CONSTRAINT sys_security_config_login_failure_window_seconds_check,
    DROP CONSTRAINT sys_security_config_login_failure_threshold_check,
    DROP CONSTRAINT sys_security_config_login_lock_duration_seconds_check;

-- +goose Down
-- Rollback rejects out-of-range data rather than silently rewriting configuration.
ALTER TABLE sys_security_config
    ADD CONSTRAINT sys_security_config_login_rate_limit_window_seconds_check
        CHECK (login_rate_limit_window_seconds BETWEEN 1 AND 3600),
    ADD CONSTRAINT sys_security_config_login_rate_limit_max_requests_check
        CHECK (login_rate_limit_max_requests BETWEEN 1 AND 10000),
    ADD CONSTRAINT sys_security_config_login_failure_window_seconds_check
        CHECK (login_failure_window_seconds BETWEEN 60 AND 86400),
    ADD CONSTRAINT sys_security_config_login_failure_threshold_check
        CHECK (login_failure_threshold BETWEEN 1 AND 100),
    ADD CONSTRAINT sys_security_config_login_lock_duration_seconds_check
        CHECK (login_lock_duration_seconds BETWEEN 60 AND 86400);
