-- +goose Up
INSERT INTO sys_api (service_name, api_group, name, path, method, is_required, status, remark)
VALUES
 ('system-api', '菜单管理', '查询菜单列表', '/menu/list', 'POST', FALSE, 1, '系统内置菜单管理接口'),
 ('system-api', '菜单管理', '查询菜单详情', '/menu/get', 'POST', FALSE, 1, '系统内置菜单管理接口'),
 ('system-api', '菜单管理', '新增菜单', '/menu/create', 'POST', FALSE, 1, '系统内置菜单管理接口'),
 ('system-api', '菜单管理', '修改菜单', '/menu/update', 'POST', FALSE, 1, '系统内置菜单管理接口'),
 ('system-api', '菜单管理', '修改菜单状态', '/menu/status/update', 'POST', FALSE, 1, '系统内置菜单管理接口'),
 ('system-api', '菜单管理', '删除菜单', '/menu/delete', 'POST', FALSE, 1, '系统内置菜单管理接口');

-- +goose Down
DELETE FROM casbin_rule WHERE ptype = 'p' AND v2 = 'POST' AND v1 IN (
 '/menu/list', '/menu/get', '/menu/create', '/menu/update', '/menu/status/update', '/menu/delete'
);
DELETE FROM sys_api WHERE method = 'POST' AND path IN (
 '/menu/list', '/menu/get', '/menu/create', '/menu/update', '/menu/status/update', '/menu/delete'
);
