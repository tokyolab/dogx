-- +goose Up
INSERT INTO sys_api (
    service_name,
    api_group,
    name,
    path,
    method,
    is_required,
    status,
    remark
)
VALUES
    (
        'system-api',
        '角色管理',
        '查询角色列表',
        '/role/list',
        'POST',
        FALSE,
        1,
        '系统内置角色查询接口'
    ),
    (
        'system-api',
        '角色管理',
        '查询角色详情',
        '/role/get',
        'POST',
        FALSE,
        1,
        '系统内置角色查询接口'
    ),
    (
        'system-api',
        '角色管理',
        '查询角色接口权限',
        '/role/api/get',
        'POST',
        FALSE,
        1,
        '系统内置角色授权查询接口'
    ),
    (
        'system-api',
        '接口管理',
        '查询接口资源列表',
        '/api/list',
        'POST',
        FALSE,
        1,
        '系统内置接口资源查询接口'
    );

INSERT INTO casbin_rule (ptype, v0, v1, v2)
SELECT
    'p',
    'r:' || role.id::TEXT,
    api.path,
    api.method
FROM sys_role AS role
CROSS JOIN sys_api AS api
WHERE role.code = 'super_admin'
  AND (api.path, api.method) IN (
      ('/role/list', 'POST'),
      ('/role/get', 'POST'),
      ('/role/api/get', 'POST'),
      ('/api/list', 'POST')
  );

-- +goose Down
DELETE FROM casbin_rule
WHERE ptype = 'p'
  AND (v1, v2) IN (
      ('/role/list', 'POST'),
      ('/role/get', 'POST'),
      ('/role/api/get', 'POST'),
      ('/api/list', 'POST')
  );

DELETE FROM sys_api
WHERE (path, method) IN (
    ('/role/list', 'POST'),
    ('/role/get', 'POST'),
    ('/role/api/get', 'POST'),
    ('/api/list', 'POST')
);
