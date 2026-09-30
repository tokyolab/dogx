-- +goose Up
CREATE TABLE sys_security_config (
 id BIGINT PRIMARY KEY CHECK (id = 1),
 login_rate_limit_enabled BOOLEAN NOT NULL DEFAULT TRUE,
 login_rate_limit_window_seconds INTEGER NOT NULL DEFAULT 60 CHECK (login_rate_limit_window_seconds BETWEEN 1 AND 3600),
 login_rate_limit_max_requests INTEGER NOT NULL DEFAULT 30 CHECK (login_rate_limit_max_requests BETWEEN 1 AND 10000),
 login_failure_lock_enabled BOOLEAN NOT NULL DEFAULT TRUE,
 login_failure_window_seconds INTEGER NOT NULL DEFAULT 900 CHECK (login_failure_window_seconds BETWEEN 60 AND 86400),
 login_failure_threshold INTEGER NOT NULL DEFAULT 5 CHECK (login_failure_threshold BETWEEN 1 AND 100),
 login_lock_duration_seconds INTEGER NOT NULL DEFAULT 900 CHECK (login_lock_duration_seconds BETWEEN 60 AND 86400),
 created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
 updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
COMMENT ON TABLE sys_security_config IS '全局安全配置，固定单行；运行时计数和锁定保存在 Redis';
COMMENT ON COLUMN sys_security_config.login_rate_limit_enabled IS '是否开启登录 IP 请求限流';
COMMENT ON COLUMN sys_security_config.login_rate_limit_window_seconds IS '请求统计窗口，秒';
COMMENT ON COLUMN sys_security_config.login_rate_limit_max_requests IS '同一 IP 在窗口内允许的登录请求次数';
COMMENT ON COLUMN sys_security_config.login_failure_lock_enabled IS '是否开启账号失败锁定，超管同样适用';
COMMENT ON COLUMN sys_security_config.login_failure_window_seconds IS '凭证失败统计窗口，秒';
COMMENT ON COLUMN sys_security_config.login_failure_threshold IS '触发锁定的失败次数';
COMMENT ON COLUMN sys_security_config.login_lock_duration_seconds IS '账号锁定时长，秒';
INSERT INTO sys_security_config(id) VALUES (1);
INSERT INTO sys_api (service_name,api_group,name,path,method,is_required,status,remark) VALUES
('system-api','安全设置','查询登录保护','/security/login/get','POST',FALSE,1,'登录保护配置'),
('system-api','安全设置','修改登录保护','/security/login/update','POST',FALSE,1,'登录保护配置');
INSERT INTO sys_menu(app_code,parent_id,menu_type,name,route_name,path,component,icon,sort)
VALUES ('admin_web',(SELECT id FROM sys_menu WHERE app_code='admin_web' AND route_name='SystemManagement' AND deleted_at IS NULL),
2,'安全设置','SecuritySettings','/system/security','system/security/index','lucide:shield-check',70);

-- +goose Down
-- +goose StatementBegin
DO $$
DECLARE security_menu_id BIGINT;
BEGIN
 SELECT id INTO security_menu_id FROM sys_menu WHERE app_code='admin_web' AND route_name='SecuritySettings' AND path='/system/security' AND deleted_at IS NULL;
 IF EXISTS (SELECT 1 FROM sys_menu WHERE parent_id=security_menu_id) THEN
  RAISE EXCEPTION 'cannot roll back security menu while child menus exist';
 END IF;
 DELETE FROM sys_role_menu WHERE menu_id=security_menu_id;
 DELETE FROM sys_menu WHERE id=security_menu_id;
END;
$$;
-- +goose StatementEnd
DELETE FROM casbin_rule WHERE ptype='p' AND v2='POST' AND v1 IN ('/security/login/get','/security/login/update');
DELETE FROM sys_api WHERE service_name='system-api' AND method='POST' AND path IN ('/security/login/get','/security/login/update');
DROP TABLE sys_security_config;
