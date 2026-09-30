-- +goose Up
COMMENT ON COLUMN sys_security_config.id IS '配置主键，固定为1';
COMMENT ON COLUMN sys_security_config.created_at IS '创建时间';
COMMENT ON COLUMN sys_security_config.updated_at IS '更新时间';
COMMENT ON COLUMN sys_login_log.user_id IS '登录时关联的用户主键，未识别到用户时为NULL；用户删除后保留原值';

-- +goose Down
COMMENT ON COLUMN sys_security_config.id IS NULL;
COMMENT ON COLUMN sys_security_config.created_at IS NULL;
COMMENT ON COLUMN sys_security_config.updated_at IS NULL;
COMMENT ON COLUMN sys_login_log.user_id IS '用户主键，未知账号或用户删除后为NULL';
