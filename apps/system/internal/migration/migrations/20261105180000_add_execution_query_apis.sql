-- +goose Up
INSERT INTO sys_api (service_name, api_group, name, path, method, is_required, status, remark)
VALUES
 ('system-api', '任务执行', '查询任务实例详情', '/job/instance/get', 'POST', FALSE, 1, '查询本轮冻结配置和执行事实'),
 ('system-api', '任务执行', '查询实例任务列表', '/job/task/list', 'POST', FALSE, 1, '分页查询执行事实，不包含业务结果数据');

-- +goose Down
DELETE FROM casbin_rule WHERE ptype='p' AND v2='POST' AND v1 IN
 ('/job/instance/get','/job/task/list');
DELETE FROM sys_api WHERE service_name='system-api' AND method='POST' AND path IN
 ('/job/instance/get','/job/task/list');
