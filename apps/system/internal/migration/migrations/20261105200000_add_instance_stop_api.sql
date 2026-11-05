-- +goose Up
INSERT INTO sys_api (service_name, api_group, name, path, method, is_required, status, remark)
VALUES ('system-api', '任务执行', '停止任务实例', '/job/instance/stop', 'POST', FALSE, 1, '请求停止本轮执行，返回已保存的状态与结束原因');

-- +goose Down
DELETE FROM casbin_rule WHERE ptype = 'p' AND v1 = '/job/instance/stop' AND v2 = 'POST';
DELETE FROM sys_api WHERE service_name = 'system-api' AND path = '/job/instance/stop' AND method = 'POST';
