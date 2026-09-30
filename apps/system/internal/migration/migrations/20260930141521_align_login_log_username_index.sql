-- +goose Up
-- Match case-insensitive exact username filtering followed by newest-ID pagination.
CREATE INDEX idx_sys_login_log_username_id
    ON sys_login_log (LOWER(username), id DESC);
DROP INDEX idx_sys_login_log_username_created_at;

-- +goose Down
CREATE INDEX idx_sys_login_log_username_created_at
    ON sys_login_log (LOWER(username), created_at DESC);
DROP INDEX idx_sys_login_log_username_id;
