-- +goose Up
ALTER TABLE sys_role
    ADD COLUMN is_system BOOLEAN NOT NULL DEFAULT FALSE,
    ADD CONSTRAINT ck_sys_role_code_format CHECK (code ~ '^[a-z][a-z0-9_]*$'),
    ADD CONSTRAINT ck_sys_role_name_not_blank CHECK (BTRIM(name) <> ''),
    ADD CONSTRAINT ck_sys_role_sort CHECK (sort >= 0);

COMMENT ON COLUMN sys_role.is_system IS '是否为系统内置角色：false否，true是';

UPDATE sys_role
SET is_system = TRUE
WHERE code = 'super_admin'
  AND deleted_at IS NULL;

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
    ('system-api', '角色管理', '新增角色', '/role/create', 'POST', FALSE, 1, '系统内置角色管理接口'),
    ('system-api', '角色管理', '修改角色', '/role/update', 'POST', FALSE, 1, '系统内置角色管理接口'),
    ('system-api', '角色管理', '修改角色状态', '/role/status/update', 'POST', FALSE, 1, '系统内置角色管理接口'),
    ('system-api', '角色管理', '删除角色', '/role/delete', 'POST', FALSE, 1, '系统内置角色管理接口');

INSERT INTO casbin_rule (ptype, v0, v1, v2)
SELECT
    'p',
    'r:' || role.id::TEXT,
    api.path,
    api.method
FROM sys_role AS role
CROSS JOIN sys_api AS api
WHERE role.code = 'super_admin'
  AND role.deleted_at IS NULL
  AND (api.path, api.method) IN (
      ('/role/create', 'POST'),
      ('/role/update', 'POST'),
      ('/role/status/update', 'POST'),
      ('/role/delete', 'POST')
  );

-- +goose Down
DELETE FROM casbin_rule
WHERE ptype = 'p'
  AND (v1, v2) IN (
      ('/role/create', 'POST'),
      ('/role/update', 'POST'),
      ('/role/status/update', 'POST'),
      ('/role/delete', 'POST')
  );

DELETE FROM sys_api
WHERE (path, method) IN (
    ('/role/create', 'POST'),
    ('/role/update', 'POST'),
    ('/role/status/update', 'POST'),
    ('/role/delete', 'POST')
);

ALTER TABLE sys_role
    DROP CONSTRAINT ck_sys_role_code_format,
    DROP CONSTRAINT ck_sys_role_name_not_blank,
    DROP CONSTRAINT ck_sys_role_sort,
    DROP COLUMN is_system;
