package model

import "time"

type Status int16
type ExecutionMode int16
type MisfirePolicy int16
type BlockPolicy int16

const (
	StatusDisabled       Status        = 0
	StatusEnabled        Status        = 1
	ExecutionModeNormal  ExecutionMode = 1
	ExecutionModeSharded ExecutionMode = 2
	MisfireIgnore        MisfirePolicy = 0
	MisfireOnce          MisfirePolicy = 1
	BlockDiscard         BlockPolicy   = 0
	BlockParallel        BlockPolicy   = 1
)

type JobDefinition struct {
	ID                   int64         `gorm:"primaryKey;autoIncrement"`
	Name                 string        `gorm:"type:varchar(128);not null"`
	Remark               string        `gorm:"type:varchar(500);not null"`
	ExecutorGroupID      int64         `gorm:"not null"`
	ProcessorName        string        `gorm:"type:varchar(128);not null"`
	ExecutionMode        ExecutionMode `gorm:"type:smallint;not null"`
	PostEnabled          bool          `gorm:"not null"`
	Params               string        `gorm:"type:text;not null"`
	CronExpr             string        `gorm:"type:varchar(128);not null"`
	Status               Status        `gorm:"type:smallint;not null"`
	MisfirePolicy        MisfirePolicy `gorm:"type:smallint;not null"`
	BlockPolicy          BlockPolicy   `gorm:"type:smallint;not null"`
	MaxRetries           int32         `gorm:"type:integer;not null"`
	RetryIntervalSeconds int32         `gorm:"type:integer;not null"`
	TimeoutSeconds       int32         `gorm:"type:integer;not null"`
	NextTriggerAt        *time.Time
	CreatedAt            time.Time `gorm:"autoCreateTime:false"`
	UpdatedAt            time.Time `gorm:"autoUpdateTime:false"`
	DeletedAt            *time.Time
}

func (JobDefinition) TableName() string { return "job_definition" }
