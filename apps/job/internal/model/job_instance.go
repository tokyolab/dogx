package model

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
)

type TriggerType int16
type InstanceStatus int16

const (
	TriggerScheduled  TriggerType    = 1
	TriggerMisfire    TriggerType    = 2
	TriggerManual     TriggerType    = 3
	InstanceWaiting   InstanceStatus = 1
	InstanceRunning   InstanceStatus = 2
	InstanceSucceeded InstanceStatus = 3
	InstanceFailed    InstanceStatus = 4
	InstanceCanceled  InstanceStatus = 5
)

// No member is omitted: false, zero retries and empty parameters are meaningful snapshots.
type ExecutionConfig struct {
	SchemaVersion        int32         `json:"schema_version"`
	ProcessorName        string        `json:"processor_name"`
	ExecutionMode        ExecutionMode `json:"execution_mode"`
	PostEnabled          bool          `json:"post_enabled"`
	Params               string        `json:"params"`
	MaxRetries           int32         `json:"max_retries"`
	RetryIntervalSeconds int32         `json:"retry_interval_seconds"`
	TimeoutSeconds       int32         `json:"timeout_seconds"`
}

func (c ExecutionConfig) Value() (driver.Value, error) {
	data, err := json.Marshal(c)
	return string(data), err
}

func (c *ExecutionConfig) Scan(value any) error {
	var data []byte
	switch v := value.(type) {
	case []byte:
		data = v
	case string:
		data = []byte(v)
	default:
		return fmt.Errorf("invalid execution config database type %T", value)
	}
	return json.Unmarshal(data, c)
}

type JobInstance struct {
	ID               int64 `gorm:"primaryKey;autoIncrement"`
	JobID            int64
	JobName          string `gorm:"type:varchar(128);not null"`
	ExecutorGroupID  int64
	TriggerType      TriggerType `gorm:"type:smallint;not null"`
	ScheduledAt      *time.Time
	RequestID        *uuid.UUID `gorm:"type:uuid"`
	ParamsOverridden bool
	TriggeredBy      *int64
	ExecutionConfig  ExecutionConfig `gorm:"type:jsonb;not null"`
	Status           InstanceStatus  `gorm:"type:smallint;not null"`
	StartedAt        *time.Time
	DeadlineAt       *time.Time
	FinishedAt       *time.Time
	EndReason        *string   `gorm:"type:varchar(64)"`
	ResultSummary    *string   `gorm:"type:text"`
	CreatedAt        time.Time `gorm:"autoCreateTime:false"`
	UpdatedAt        time.Time `gorm:"autoUpdateTime:false"`
}

func (JobInstance) TableName() string { return "job_instance" }
