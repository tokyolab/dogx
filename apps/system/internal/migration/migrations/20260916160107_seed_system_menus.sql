-- +goose Up
-- Only DogX-owned system pages belong to this seed. Vben demo and profile routes stay frontend-only.
-- Conflicting route names or paths fail the migration instead of overwriting existing menu data.
WITH system_directory AS (
    INSERT INTO sys_menu (app_code, menu_type, name, route_name, path, icon, sort)
    VALUES ('admin_web', 1, '系统管理', 'SystemManagement', '/system', 'lucide:settings', 10)
    RETURNING id
)
INSERT INTO sys_menu (app_code, parent_id, menu_type, name, route_name, path, component, icon, sort)
SELECT 'admin_web', system_directory.id, 2, page.name, page.route_name, page.path, page.component, page.icon, page.sort
FROM system_directory
CROSS JOIN (VALUES
    ('菜单管理', 'MenuManagement', '/system/menu', 'system/menu/index', 'lucide:menu', 10),
    ('用户管理', 'UserManagement', '/system/user', 'system/user/index', 'lucide:user', 20),
    ('角色管理', 'RoleManagement', '/system/role', 'system/role/index', 'lucide:shield-check', 30)
) AS page(name, route_name, path, component, icon, sort);

-- +goose Down
-- Do not orphan menus added beneath the seed nodes after initialization.
-- +goose StatementBegin
DO $$
DECLARE
    menu_ids BIGINT[];
BEGIN
    SELECT array_agg(id) INTO menu_ids
    FROM sys_menu
    WHERE app_code = 'admin_web' AND deleted_at IS NULL
      AND (route_name, path) IN (
          ('SystemManagement', '/system'),
          ('MenuManagement', '/system/menu'),
          ('UserManagement', '/system/user'),
          ('RoleManagement', '/system/role')
      );

    IF EXISTS (
        SELECT 1 FROM sys_menu
        WHERE parent_id = ANY(menu_ids) AND NOT (id = ANY(menu_ids))
    ) THEN
        RAISE EXCEPTION 'cannot roll back system menu seed while other menus reference it';
    END IF;

    DELETE FROM sys_role_menu WHERE menu_id = ANY(menu_ids);
    DELETE FROM sys_menu WHERE id = ANY(menu_ids);
END;
$$;
-- +goose StatementEnd
