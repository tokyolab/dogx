-- +goose Up
INSERT INTO sys_api (service_name, api_group, name, path, method, is_required, status, remark)
VALUES
 ('system-api', '任务管理', '查询任务列表', '/job/list', 'POST', FALSE, 1, 'Job管理'),
 ('system-api', '任务管理', '查询任务详情', '/job/get', 'POST', FALSE, 1, 'Job管理'),
 ('system-api', '任务管理', '新增任务', '/job/create', 'POST', FALSE, 1, 'Job管理'),
 ('system-api', '任务管理', '编辑任务', '/job/update', 'POST', FALSE, 1, 'Job管理'),
 ('system-api', '任务管理', '启停任务', '/job/status/update', 'POST', FALSE, 1, 'Job管理'),
 ('system-api', '任务管理', '删除任务', '/job/delete', 'POST', FALSE, 1, 'Job管理'),
 ('system-api', '任务管理', '预览任务Cron', '/job/cron/preview', 'POST', FALSE, 1, 'Job管理'),
 ('system-api', '任务管理', '手动触发任务', '/job/trigger', 'POST', FALSE, 1, '创建待开始Instance');

-- +goose Down
DELETE FROM casbin_rule WHERE ptype='p' AND v2='POST' AND v1 IN
 ('/job/list','/job/get','/job/create','/job/update','/job/status/update','/job/delete','/job/cron/preview','/job/trigger');
DELETE FROM sys_api WHERE service_name='system-api' AND method='POST' AND path IN
 ('/job/list','/job/get','/job/create','/job/update','/job/status/update','/job/delete','/job/cron/preview','/job/trigger');
