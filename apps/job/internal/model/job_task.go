package model

import (
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// TaskType identifies the processing stage of one Task, not the Instance's
// execution mode. Ordinary, prepare and post Tasks are singletons per Instance.
type TaskType int16

const (
	TaskTypeOrdinary TaskType = 1
	TaskTypePrepare  TaskType = 2
	TaskTypeShard    TaskType = 3
	TaskTypePost     TaskType = 4
)

// TaskStatus describes one Task. Its numeric values are independent of
// InstanceStatus; a retry-waiting Task has closed its previous attempt but has
// not reached a terminal state.
type TaskStatus int16

const (
	TaskWaiting      TaskStatus = 1
	TaskRunning      TaskStatus = 2
	TaskRetryWaiting TaskStatus = 3
	TaskSucceeded    TaskStatus = 4
	TaskFailed       TaskStatus = 5
	TaskCanceled     TaskStatus = 6
)

const (
	// EndReasonSuccess records a final successful execution result.
	EndReasonSuccess = "success"
	// EndReasonExecutionFailed records a final failure after execution
	// opportunities are exhausted, not a retry-waiting attempt or RPC error.
	EndReasonExecutionFailed = "execution_failed"
	// EndReasonDispatchRejected records exhausted execution opportunities after
	// a definite pre-admission rejection.
	EndReasonDispatchRejected = "dispatch_rejected"
	// EndReasonResultUnknown records exhausted execution opportunities after an
	// unbound attempt's observation period. It does not prove business failure.
	EndReasonResultUnknown = "result_unknown"
	// EndReasonManualStop freezes an operator's whole-Instance stop decision.
	// Unfinished Tasks inherit it when canceled; it does not prove business exit.
	EndReasonManualStop = "manual_stop"
	// EndReasonInstanceTimeout freezes an Instance deadline stop decision.
	// The Instance finishes failed, while its unfinished Tasks finish canceled.
	EndReasonInstanceTimeout = "instance_timeout"
)

// TaskData preserves Task input and output JSON without decoding numbers into floating
// point. A nil slice represents SQL NULL; the bytes "null" represent JSON null.
// Value checks JSON syntax only; PostgreSQL remains responsible for JSONB storage
// restrictions such as unsupported Unicode escapes and numeric range.
type TaskData json.RawMessage

// Value implements driver.Valuer. Non-nil empty or invalid JSON is rejected;
// valid JSON is passed as text without changing its numeric precision.
func (d TaskData) Value() (driver.Value, error) {
	if d == nil {
		return nil, nil
	}
	if !json.Valid(d) {
		return nil, errors.New("invalid task JSON")
	}
	return string(d), nil
}

// Scan implements sql.Scanner. It accepts SQL NULL, string and []byte values,
// copies driver-owned bytes, and leaves the receiver unchanged on failure.
// PostgreSQL may normalize JSONB formatting; the value's numeric precision and
// distinction between SQL NULL and JSON null are preserved.
func (d *TaskData) Scan(value any) error {
	if value == nil {
		*d = nil
		return nil
	}
	var data []byte
	switch v := value.(type) {
	case []byte:
		data = v
	case string:
		data = []byte(v)
	default:
		return fmt.Errorf("invalid task database type %T", value)
	}
	if !json.Valid(data) {
		return errors.New("invalid task JSON")
	}
	*d = append(TaskData(nil), data...)
	return nil
}

// JobTask stores execution facts for one Task. Its parent Instance owns the
// frozen group, processor, parameters, retry policy and round deadline. Retries
// update this row rather than creating attempt history. There is no soft-delete
// lifecycle, so the model does not embed the shared Base.
type JobTask struct {
	ID         int64      `gorm:"primaryKey;autoIncrement"`
	InstanceID int64      `gorm:"not null"`
	TaskType   TaskType   `gorm:"type:smallint;not null"`
	Status     TaskStatus `gorm:"type:smallint;not null"`
	// AttemptNo identifies dispatches and never moves backward. UsedAttempts
	// counts occupied execution opportunities and may be refunded by definite BUSY.
	AttemptNo    int64 `gorm:"not null"`
	UsedAttempts int32 `gorm:"not null"`
	// AttemptRegisteredAt measures the current attempt's missing-binding
	// observation period. StartedAt retains the first registration across retries.
	AttemptRegisteredAt *time.Time
	NextDispatchAt      *time.Time
	ExecutorInstanceID  *uuid.UUID `gorm:"type:uuid"`
	ExecutorAddress     *string    `gorm:"type:varchar(255)"`
	// InputData is the immutable shard input or Post statistics for all attempts.
	// Ordinary and prepare Tasks leave it SQL NULL; JSON null is a valid shard input.
	InputData  TaskData `gorm:"type:jsonb"`
	ResultData TaskData `gorm:"type:jsonb"`
	// ResultSummary can describe the previous attempt after a retry starts; it
	// does not prove that the current attempt has completed.
	ResultSummary *string `gorm:"type:text"`
	EndReason     *string `gorm:"type:varchar(64)"`
	StartedAt     *time.Time
	FinishedAt    *time.Time
	CreatedAt     time.Time `gorm:"autoCreateTime:false"`
	UpdatedAt     time.Time `gorm:"autoUpdateTime:false"`
}

func (JobTask) TableName() string { return "job_task" }
