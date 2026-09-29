-- +goose Up
INSERT INTO sys_api (service_name, api_group, name, path, method, is_required, status, remark)
VALUES ('system-api', '登录日志', '查询登录日志', '/login-log/list', 'POST', FALSE, 1, '系统登录审计查询接口');

INSERT INTO sys_menu (app_code, parent_id, menu_type, name, route_name, path, component, icon, sort)
VALUES ('admin_web', (SELECT id FROM sys_menu WHERE app_code = 'admin_web' AND route_name = 'SystemManagement' AND deleted_at IS NULL),
        2, '登录日志', 'LoginLog', '/system/login-log', 'system/login-log/index', 'lucide:log-in', 60);

-- +goose Down
-- +goose StatementBegin
DO $$
DECLARE login_log_menu_id BIGINT;
BEGIN
 SELECT id INTO login_log_menu_id FROM sys_menu WHERE app_code='admin_web' AND route_name='LoginLog' AND path='/system/login-log' AND deleted_at IS NULL;
 IF EXISTS (SELECT 1 FROM sys_menu WHERE parent_id=login_log_menu_id) THEN
  RAISE EXCEPTION 'cannot roll back login log menu while child menus exist';
 END IF;
 DELETE FROM sys_role_menu WHERE menu_id=login_log_menu_id;
 DELETE FROM sys_menu WHERE id=login_log_menu_id;
END;
$$;
-- +goose StatementEnd
DELETE FROM casbin_rule WHERE ptype='p' AND v1='/login-log/list' AND v2='POST';
DELETE FROM sys_api WHERE service_name='system-api' AND path='/login-log/list' AND method='POST';
