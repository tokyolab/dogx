-- +goose Up
INSERT INTO sys_api (service_name, api_group, name, path, method, is_required, status, remark)
VALUES
 ('system-api', '任务执行', '查询执行记录', '/job/instance/list', 'POST', FALSE, 1, '按本轮快照和创建时间查询执行历史'),
 ('system-api', '任务执行', '查询执行任务详情', '/job/task/get', 'POST', FALSE, 1, '按需读取单份任务输入或结果');

-- Keep scheduling beside System management. Existing custom menus are never overwritten.
WITH scheduling AS (
 INSERT INTO sys_menu (app_code, menu_type, name, route_name, path, icon, sort)
 VALUES ('admin_web', 1, '定时任务', 'JobScheduling', '/job', 'lucide:clock', 20)
 RETURNING id
)
INSERT INTO sys_menu (app_code, parent_id, menu_type, name, route_name, path, component, icon, sort)
SELECT 'admin_web', scheduling.id, 2, page.name, page.route_name, page.path, page.component, page.icon, page.sort
FROM scheduling
CROSS JOIN (VALUES
 ('任务管理', 'JobManagement', '/job/definition', 'job/definition/index', 'lucide:calendar-clock', 10),
 ('执行记录', 'JobExecutions', '/job/execution', 'job/execution/index', 'lucide:list-checks', 20),
 ('执行器组', 'ExecutorGroups', '/job/group', 'job/group/index', 'lucide:network', 30)
) AS page(name, route_name, path, component, icon, sort);

-- +goose Down
-- +goose StatementBegin
DO $$
DECLARE menu_ids BIGINT[];
BEGIN
 SELECT array_agg(id) INTO menu_ids FROM sys_menu
 WHERE app_code='admin_web' AND
  ((route_name='JobScheduling' AND path='/job') OR
   (route_name='JobManagement' AND path='/job/definition') OR
   (route_name='JobExecutions' AND path='/job/execution') OR
   (route_name='ExecutorGroups' AND path='/job/group'));
 IF EXISTS (SELECT 1 FROM sys_menu WHERE parent_id=ANY(menu_ids) AND NOT (id=ANY(menu_ids))) THEN
  RAISE EXCEPTION 'cannot roll back scheduled job menus while custom children exist';
 END IF;
 DELETE FROM sys_role_menu WHERE menu_id=ANY(menu_ids);
 DELETE FROM sys_menu WHERE id=ANY(menu_ids);
END;
$$;
-- +goose StatementEnd
DELETE FROM casbin_rule WHERE ptype='p' AND v2='POST' AND v1 IN ('/job/instance/list','/job/task/get');
DELETE FROM sys_api WHERE service_name='system-api' AND method='POST' AND path IN ('/job/instance/list','/job/task/get');
