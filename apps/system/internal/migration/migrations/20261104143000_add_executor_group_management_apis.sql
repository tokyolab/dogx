-- +goose Up
INSERT INTO sys_api (service_name, api_group, name, path, method, is_required, status, remark)
VALUES
 ('system-api', '执行器组', '创建执行器组', '/job/group/create', 'POST', FALSE, 1, '后端生成组Token'),
 ('system-api', '执行器组', '查询执行器组详情', '/job/group/get', 'POST', FALSE, 1, '包含完整组Token'),
 ('system-api', '执行器组', '查询执行器组列表', '/job/group/list', 'POST', FALSE, 1, '不包含组Token'),
 ('system-api', '执行器组', '删除执行器组', '/job/group/delete', 'POST', FALSE, 1, '检查引用后逻辑删除');

-- +goose Down
DELETE FROM casbin_rule WHERE ptype='p' AND v2='POST' AND v1 IN
 ('/job/group/create','/job/group/get','/job/group/list','/job/group/delete');
DELETE FROM sys_api WHERE service_name='system-api' AND method='POST' AND path IN
 ('/job/group/create','/job/group/get','/job/group/list','/job/group/delete');
