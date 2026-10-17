package model

import (
	"time"

	basemodel "github.com/tokyolab/dogx/pkg/model"
)

// ExecutionMode distinguishes normal execution from execution over shards
// prepared by a business processor. It is not a routing policy.
type ExecutionMode int16

// MisfirePolicy chooses whether to ignore missed occurrences or coalesce them
// into one round when an automatic trigger exceeds the lateness tolerance.
type MisfirePolicy int16

// BlockPolicy controls whether a new round may be created while another
// Instance is waiting or running.
type BlockPolicy int16

const (
	ExecutionModeNormal  ExecutionMode = 1
	ExecutionModeSharded ExecutionMode = 2
	MisfireIgnore        MisfirePolicy = 0
	MisfireOnce          MisfirePolicy = 1
	BlockDiscard         BlockPolicy   = 0
	BlockParallel        BlockPolicy   = 1
)

// JobDefinition stores current management configuration and automatic-trigger
// progress; existing Instances use their own snapshots. Base timestamps are
// audit data only. NextTriggerAt tracks which scheduled trigger is unprocessed.
type JobDefinition struct {
	basemodel.Base
	Name            string `gorm:"type:varchar(128);not null"`
	Remark          string `gorm:"type:varchar(500);not null"`
	ExecutorGroupID int64  `gorm:"not null"`
	// ProcessorName is a case-sensitive handler identifier within the executor
	// group, not an RPC address or path.
	ProcessorName string                 `gorm:"type:varchar(128);not null"`
	ExecutionMode ExecutionMode          `gorm:"type:smallint;not null"`
	PostEnabled   bool                   `gorm:"not null"`
	Params        string                 `gorm:"type:text;not null"`
	CronExpr      string                 `gorm:"type:varchar(128);not null"`
	Status        basemodel.RecordStatus `gorm:"type:smallint;not null"`
	MisfirePolicy MisfirePolicy          `gorm:"type:smallint;not null"`
	BlockPolicy   BlockPolicy            `gorm:"type:smallint;not null"`
	// MaxRetries allows additional execution attempts per Task, excluding the
	// initial attempt and result-reporting retries.
	MaxRetries           int32 `gorm:"type:integer;not null"`
	RetryIntervalSeconds int32 `gorm:"type:integer;not null"`
	// TimeoutSeconds budgets the whole round from its first effective start,
	// including preparation, execution, and post-processing.
	TimeoutSeconds int32 `gorm:"type:integer;not null"`
	// NextTriggerAt is the full timestamp of the next trigger whose accounting
	// is unfinished, not the previous execution's finish time. It is nil before
	// first enabling the Job, retained on disable, and unchanged by manual triggers.
	NextTriggerAt *time.Time
}

func (JobDefinition) TableName() string { return "job_definition" }
