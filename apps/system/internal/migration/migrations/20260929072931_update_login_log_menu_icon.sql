-- +goose Up
UPDATE sys_menu SET icon = 'lucide:history'
WHERE app_code = 'admin_web' AND route_name = 'LoginLog'
  AND path = '/system/login-log' AND deleted_at IS NULL AND icon = 'lucide:log-in';

-- +goose Down
UPDATE sys_menu SET icon = 'lucide:log-in'
WHERE app_code = 'admin_web' AND route_name = 'LoginLog'
  AND path = '/system/login-log' AND deleted_at IS NULL AND icon = 'lucide:history';
