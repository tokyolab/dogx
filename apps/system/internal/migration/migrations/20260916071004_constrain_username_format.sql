-- +goose Up
-- Keep letter ranges ASCII-only regardless of the database locale.
ALTER TABLE sys_user
    ADD CONSTRAINT ck_sys_user_username_format
    CHECK (username COLLATE "C" ~ '^[A-Za-z0-9]+(-[A-Za-z0-9]+)*$');

-- +goose Down
ALTER TABLE sys_user DROP CONSTRAINT ck_sys_user_username_format;
