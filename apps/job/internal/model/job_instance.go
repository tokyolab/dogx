package model

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// TriggerType identifies a regular schedule, misfire recovery, or manual
// trigger. It does not distinguish Cron callbacks from recovery scans.
type TriggerType int16

// InstanceStatus describes the whole round, not a phase or individual Task.
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

// ExecutionConfig is the JSONB execution snapshot fixed at Instance creation;
// later Job edits do not change this round. Every member is serialized: false,
// zero retries, and empty parameters are meaningful, so omitempty is not used.
// SchemaVersion identifies the snapshot format. Value and Scan only convert
// the storage representation; they do not validate versions or business values.
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

// Value implements driver.Valuer by encoding the snapshot as a JSON string.
// Encoding failures are returned as errors.
func (c ExecutionConfig) Value() (driver.Value, error) {
	data, err := json.Marshal(c)
	return string(data), err
}

// Scan implements sql.Scanner and accepts only []byte or string values, not SQL
// NULL. JSON decoding errors are returned unchanged. A failed decode may have
// partially modified the receiver; callers must not use that result.
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

// JobInstance stores a round's fixed configuration and execution facts. It has
// no soft-delete lifecycle and therefore does not embed the shared Base.
// The trigger component delivers InstanceWaiting; inserting the record does not
// start execution or the round timeout. Automatic identities are unique by
// (JobID, ScheduledAt), and manual identities by (JobID, RequestID).
type JobInstance struct {
	ID              int64 `gorm:"primaryKey;autoIncrement"`
	JobID           int64
	JobName         string `gorm:"type:varchar(128);not null"`
	ExecutorGroupID int64
	TriggerType     TriggerType `gorm:"type:smallint;not null"`
	// ScheduledAt retains the complete previous NextTriggerAt read under the
	// Job lock. It is nil for manual triggers.
	ScheduledAt *time.Time
	// RequestID is the UUID reused for manual-trigger retries for the same Job.
	// It is nil for automatic triggers.
	RequestID        *uuid.UUID `gorm:"type:uuid"`
	ParamsOverridden bool
	TriggeredBy      *int64
	ExecutionConfig  ExecutionConfig `gorm:"type:jsonb;not null"`
	Status           InstanceStatus  `gorm:"type:smallint;not null"`
	// StartedAt records the first effective start; DeadlineAt stores the round
	// deadline fixed at that start. Both begin nil. Execution retries do not
	// restart the round's time budget.
	StartedAt  *time.Time
	DeadlineAt *time.Time
	FinishedAt *time.Time
	// EndReason is a stable completion reason code, not a full error message.
	// ResultSummary is not a store for detailed execution logs.
	EndReason     *string   `gorm:"type:varchar(64)"`
	ResultSummary *string   `gorm:"type:text"`
	CreatedAt     time.Time `gorm:"autoCreateTime:false"`
	UpdatedAt     time.Time `gorm:"autoUpdateTime:false"`
}

func (JobInstance) TableName() string { return "job_instance" }
