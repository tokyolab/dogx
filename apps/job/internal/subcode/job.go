package subcode

const (
	InstanceNotFound                = "job.instance.not_found"
	JobNotFound                     = "job.definition.not_found"
	JobStateNotAllowed              = "job.definition.state_not_allowed"
	JobActiveInstances              = "job.definition.active_instances"
	JobExecutorGroupUnavailable     = "job.definition.executor_group_unavailable"
	JobTriggerBlocked               = "job.trigger.blocked"
	JobTriggerRequestConflict       = "job.trigger.request_conflict"
	JobTriggerCapacityBusy          = "job.trigger.capacity_busy"
	ExecutorGroupNotFound           = "job.executor_group.not_found"
	ExecutorGroupNameExists         = "job.executor_group.name_exists"
	ExecutorGroupDiscoveryKeyExists = "job.executor_group.discovery_key_exists"
	ExecutorGroupInUse              = "job.executor_group.in_use"
)
