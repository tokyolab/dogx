-- +goose Up
INSERT INTO sys_api (service_name, api_group, name, path, method, is_required, status, remark)
VALUES
    ('system-api', '用户管理', '查询用户列表', '/user/list', 'POST', FALSE, 1, '系统内置用户管理接口'),
    ('system-api', '用户管理', '查询用户详情', '/user/get', 'POST', FALSE, 1, '系统内置用户管理接口'),
    ('system-api', '用户管理', '新增用户', '/user/create', 'POST', FALSE, 1, '系统内置用户管理接口'),
    ('system-api', '用户管理', '修改用户资料', '/user/update', 'POST', FALSE, 1, '系统内置用户管理接口'),
    ('system-api', '用户管理', '修改用户状态', '/user/status/update', 'POST', FALSE, 1, '系统内置用户管理接口'),
    ('system-api', '用户管理', '删除用户', '/user/delete', 'POST', FALSE, 1, '系统内置用户管理接口'),
    ('system-api', '用户管理', '分配用户角色', '/user/role/update', 'POST', FALSE, 1, '系统内置用户管理接口'),
    ('system-api', '用户管理', '重置用户密码', '/user/password/reset', 'POST', FALSE, 1, '系统内置用户管理接口'),
    ('system-api', '用户管理', '查询可分配角色', '/user/role/options', 'POST', FALSE, 1, '系统内置用户管理接口');

-- +goose Down
DELETE FROM casbin_rule WHERE ptype = 'p' AND v2 = 'POST' AND v1 IN (
    '/user/list', '/user/get', '/user/create', '/user/update', '/user/status/update',
    '/user/delete', '/user/role/update', '/user/password/reset', '/user/role/options'
);
DELETE FROM sys_api WHERE method = 'POST' AND path IN (
    '/user/list', '/user/get', '/user/create', '/user/update', '/user/status/update',
    '/user/delete', '/user/role/update', '/user/password/reset', '/user/role/options'
);
