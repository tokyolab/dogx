-- +goose Up
ALTER TABLE sys_role
    ADD CONSTRAINT ck_sys_role_super_admin_system CHECK (
        code <> 'super_admin' OR is_system = TRUE
    );

-- +goose Down
ALTER TABLE sys_role
    DROP CONSTRAINT ck_sys_role_super_admin_system;
