-- +goose Up
DROP INDEX IF EXISTS idx_sys_user_deleted_at;
DROP INDEX IF EXISTS idx_sys_role_deleted_at;
DROP INDEX IF EXISTS idx_sys_menu_deleted_at;
DROP INDEX IF EXISTS idx_sys_menu_permission;
DROP INDEX IF EXISTS idx_sys_api_deleted_at;

DROP INDEX IF EXISTS uk_sys_role_code_active;
CREATE UNIQUE INDEX uk_sys_role_code_active
    ON sys_role (code)
    WHERE deleted_at IS NULL;

DROP INDEX IF EXISTS idx_sys_user_role_role_id;
CREATE INDEX idx_sys_user_role_role_id
    ON sys_user_role (role_id, user_id);

-- +goose Down
DROP INDEX IF EXISTS idx_sys_user_role_role_id;
CREATE INDEX idx_sys_user_role_role_id
    ON sys_user_role (role_id);

DROP INDEX IF EXISTS uk_sys_role_code_active;
CREATE UNIQUE INDEX uk_sys_role_code_active
    ON sys_role (LOWER(code))
    WHERE deleted_at IS NULL;

CREATE INDEX idx_sys_user_deleted_at ON sys_user (deleted_at);
CREATE INDEX idx_sys_role_deleted_at ON sys_role (deleted_at);
CREATE INDEX idx_sys_menu_deleted_at ON sys_menu (deleted_at);
CREATE INDEX idx_sys_menu_permission ON sys_menu (permission)
    WHERE permission <> '' AND deleted_at IS NULL;
CREATE INDEX idx_sys_api_deleted_at ON sys_api (deleted_at);
