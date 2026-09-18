-- +goose Up
INSERT INTO sys_api (service_name, api_group, name, path, method, is_required, status, remark)
VALUES
 ('system-api', '角色管理', '查询角色菜单权限', '/role/menu/get', 'POST', FALSE, 1, '系统内置角色菜单授权接口'),
 ('system-api', '角色管理', '修改角色菜单权限', '/role/menu/update', 'POST', FALSE, 1, '系统内置角色菜单授权接口');

-- +goose Down
DELETE FROM casbin_rule WHERE ptype = 'p' AND v2 = 'POST' AND v1 IN ('/role/menu/get', '/role/menu/update');
DELETE FROM sys_api WHERE method = 'POST' AND path IN ('/role/menu/get', '/role/menu/update');
