-- +goose Up
INSERT INTO sys_role (code, name, description, sort, status)
VALUES ('super_admin', '超级管理员', '系统初始化超级管理员角色', 0, 1);

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
VALUES (
    'system-api',
    '角色管理',
    '更新角色接口权限',
    '/role/api/update',
    'POST',
    FALSE,
    1,
    '系统内置权限管理接口'
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
  AND api.path = '/role/api/update'
  AND api.method = 'POST';

-- +goose Down
DELETE FROM casbin_rule
WHERE ptype = 'p'
  AND v0 IN (
      SELECT 'r:' || id::TEXT
      FROM sys_role
      WHERE code = 'super_admin'
  )
  AND v1 = '/role/api/update'
  AND v2 = 'POST';

DELETE FROM sys_api
WHERE path = '/role/api/update'
  AND method = 'POST';

DELETE FROM sys_user_role
WHERE role_id IN (
    SELECT id
    FROM sys_role
    WHERE code = 'super_admin'
);

DELETE FROM sys_role
WHERE code = 'super_admin';
