-- +goose Up
DELETE FROM casbin_rule
WHERE ptype = 'p'
  AND v0 IN (
      SELECT 'r:' || id::text
      FROM sys_role
      WHERE code = 'super_admin'
        AND deleted_at IS NULL
  );

-- +goose Down
INSERT INTO casbin_rule (ptype, v0, v1, v2)
SELECT 'p', 'r:' || role.id::text, api.path, api.method
FROM sys_role AS role
CROSS JOIN sys_api AS api
WHERE role.code = 'super_admin'
  AND role.status = 1
  AND role.deleted_at IS NULL
  AND api.status = 1
  AND api.deleted_at IS NULL
ON CONFLICT DO NOTHING;
