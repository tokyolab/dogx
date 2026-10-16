-- +goose Up
COMMENT ON COLUMN job_executor_group.updated_at IS '记录最后更新时间，名称修改等常规更新时维护，不作为业务配置版本；逻辑删除时间由deleted_at记录';
COMMENT ON COLUMN job_definition.updated_at IS '记录最后更新时间，配置修改、触发推进等常规更新时维护，不作为业务配置版本；逻辑删除时间由deleted_at记录';
COMMENT ON COLUMN job_instance.updated_at IS '记录最后更新时间，含运行状态和结果变更，不作为业务配置版本';

-- +goose Down
COMMENT ON COLUMN job_executor_group.updated_at IS '管理修改时间，逻辑删除时一并更新';
COMMENT ON COLUMN job_definition.updated_at IS '管理配置最后修改时间，仅推进触发进度不更新';
COMMENT ON COLUMN job_instance.updated_at IS '有效运行事实最后修改时间';
