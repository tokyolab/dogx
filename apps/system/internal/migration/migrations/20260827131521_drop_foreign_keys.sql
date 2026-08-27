-- +goose Up
ALTER TABLE sys_menu
    DROP CONSTRAINT IF EXISTS fk_sys_menu_parent,
    DROP CONSTRAINT IF EXISTS uk_sys_menu_id_app_code;

ALTER TABLE sys_user_role
    DROP CONSTRAINT IF EXISTS fk_sys_user_role_user,
    DROP CONSTRAINT IF EXISTS fk_sys_user_role_role;

ALTER TABLE sys_role_menu
    DROP CONSTRAINT IF EXISTS fk_sys_role_menu_role,
    DROP CONSTRAINT IF EXISTS fk_sys_role_menu_menu;

ALTER TABLE sys_login_log
    DROP CONSTRAINT IF EXISTS fk_sys_login_log_user;

-- +goose Down
ALTER TABLE sys_menu
    ADD CONSTRAINT uk_sys_menu_id_app_code UNIQUE (id, app_code);

ALTER TABLE sys_menu
    ADD CONSTRAINT fk_sys_menu_parent FOREIGN KEY (parent_id, app_code)
        REFERENCES sys_menu (id, app_code) ON DELETE RESTRICT;

ALTER TABLE sys_user_role
    ADD CONSTRAINT fk_sys_user_role_user FOREIGN KEY (user_id)
        REFERENCES sys_user (id) ON DELETE CASCADE,
    ADD CONSTRAINT fk_sys_user_role_role FOREIGN KEY (role_id)
        REFERENCES sys_role (id) ON DELETE CASCADE;

ALTER TABLE sys_role_menu
    ADD CONSTRAINT fk_sys_role_menu_role FOREIGN KEY (role_id)
        REFERENCES sys_role (id) ON DELETE CASCADE,
    ADD CONSTRAINT fk_sys_role_menu_menu FOREIGN KEY (menu_id)
        REFERENCES sys_menu (id) ON DELETE CASCADE;

ALTER TABLE sys_login_log
    ADD CONSTRAINT fk_sys_login_log_user FOREIGN KEY (user_id)
        REFERENCES sys_user (id) ON DELETE SET NULL;
