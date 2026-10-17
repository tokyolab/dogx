package model

import (
	"time"

	basemodel "github.com/tokyolab/dogx/pkg/model"
)

type ExecutionMode int16
type MisfirePolicy int16
type BlockPolicy int16

const (
	ExecutionModeNormal  ExecutionMode = 1
	ExecutionModeSharded ExecutionMode = 2
	MisfireIgnore        MisfirePolicy = 0
	MisfireOnce          MisfirePolicy = 1
	BlockDiscard         BlockPolicy   = 0
	BlockParallel        BlockPolicy   = 1
)

type JobDefinition struct {
	basemodel.Base
	Name                 string                 `gorm:"type:varchar(128);not null"`
	Remark               string                 `gorm:"type:varchar(500);not null"`
	ExecutorGroupID      int64                  `gorm:"not null"`
	ProcessorName        string                 `gorm:"type:varchar(128);not null"`
	ExecutionMode        ExecutionMode          `gorm:"type:smallint;not null"`
	PostEnabled          bool                   `gorm:"not null"`
	Params               string                 `gorm:"type:text;not null"`
	CronExpr             string                 `gorm:"type:varchar(128);not null"`
	Status               basemodel.RecordStatus `gorm:"type:smallint;not null"`
	MisfirePolicy        MisfirePolicy          `gorm:"type:smallint;not null"`
	BlockPolicy          BlockPolicy            `gorm:"type:smallint;not null"`
	MaxRetries           int32                  `gorm:"type:integer;not null"`
	RetryIntervalSeconds int32                  `gorm:"type:integer;not null"`
	TimeoutSeconds       int32                  `gorm:"type:integer;not null"`
	NextTriggerAt        *time.Time
}

func (JobDefinition) TableName() string { return "job_definition" }
