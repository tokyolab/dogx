package repository

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/tokyolab/dogx/apps/job/internal/model"
	basemodel "github.com/tokyolab/dogx/pkg/model"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// ErrExecutorGroupUnavailable means the group is missing or soft-deleted.
// It does not mean that the group has no online executors.
var ErrExecutorGroupUnavailable = errors.New("executor group is unavailable")

var (
	// ErrExecutorGroupNameExists includes names retained by soft-deleted groups.
	ErrExecutorGroupNameExists = errors.New("executor group name exists")
	// ErrExecutorGroupDiscoveryKeyExists includes keys retained by soft-deleted groups.
	ErrExecutorGroupDiscoveryKeyExists = errors.New("executor group discovery key exists")
)

// ExecutorGroupFilter uses one-based pagination with PageSize from 1 through 200.
// Name is an optional case-insensitive literal substring, not a SQL pattern.
type ExecutorGroupFilter struct {
	Page     int64
	PageSize int64
	Name     string
}

// ExecutorGroupListItem deliberately excludes credentials from management lists.
type ExecutorGroupListItem struct {
	ID           int64
	Name         string
	DiscoveryKey string
	CreatedAt    time.Time
	UpdatedAt    time.Time
}

// ExecutorGroupConfig contains only the fields needed for execution routing and
// authentication. Token is sensitive; do not log or expose this projection in lists.
type ExecutorGroupConfig struct {
	ID           int64
	Name         string
	DiscoveryKey string
	Token        string
}

// ExecutorGroupRepository persists immutable group configuration and reads active
// groups. It does not perform service discovery or check executor liveness.
// Callers validate inputs and generate credentials before creation. A successful
// transaction-scoped write does not confirm the outer transaction's commit.
type ExecutorGroupRepository interface {
	// Create inserts complete configuration, ignoring the supplied Base and writing
	// the generated Base back to group. The non-nil input must not be accessed
	// concurrently; rollback does not undo its in-memory Base. Names and discovery
	// keys are unique across both active and soft-deleted records.
	Create(context.Context, *model.ExecutorGroup) (int64, error)

	// FindByID returns details including Token, or nil, ErrExecutorGroupUnavailable
	// for a missing or deleted group. It never returns nil, nil.
	FindByID(context.Context, int64) (*model.ExecutorGroup, error)

	// Delete soft-deletes the group without changing its immutable configuration
	// or UpdatedAt. Callers must first LockAvailable, check undeleted Job references,
	// and then check active Instances in separate statements of the same transaction.
	// The group lock prevents new references while those checks and this write run.
	// Missing or already deleted groups return ErrExecutorGroupUnavailable.
	Delete(context.Context, int64) error

	// List returns descending-ID management rows and a count from separate queries.
	// The queries need not observe one snapshot; an empty page is a non-nil slice.
	// Neither the SQL projection nor the returned rows includes Token.
	List(context.Context, ExecutorGroupFilter) ([]ExecutorGroupListItem, int64, error)

	// LoadExecutionGroups returns all active execution configurations from one
	// query. An empty result is a non-nil slice; errors never return a partial set.
	LoadExecutionGroups(context.Context) ([]ExecutorGroupConfig, error)

	// FindExecutionGroupByID returns execution configuration or nil,
	// ErrExecutorGroupUnavailable. It never returns nil, nil.
	FindExecutionGroupByID(context.Context, int64) (*ExecutorGroupConfig, error)

	// FindExecutionGroupByName uses the exact, case-sensitive group name. Missing
	// and deleted names return nil, ErrExecutorGroupUnavailable, never nil, nil.
	FindExecutionGroupByName(context.Context, string) (*ExecutorGroupConfig, error)

	// LockAvailable locks the group by primary key and succeeds only if the group
	// exists and is not deleted. Call it in an explicit transaction before writing
	// a Job reference; the lock lasts until that transaction ends. Lock the group
	// before the Job to preserve the order used by group-deletion reference checks.
	// Missing or deleted groups return ErrExecutorGroupUnavailable; other database
	// errors preserve their underlying causes.
	LockAvailable(context.Context, int64) error

	// Available reports whether the group exists and is not deleted in the query
	// snapshot, returning false, nil otherwise. It takes no lock and does not
	// guarantee later availability, so it cannot replace LockAvailable when
	// creating or changing a Job reference.
	Available(context.Context, int64) (bool, error)
}

type executorGroupRepository struct{ db *gorm.DB }

// NewExecutorGroupRepository binds a non-nil database or transaction handle.
// It does not check connectivity, close the connection, or commit transactions.
// Use TransactionRunner to obtain a transaction-scoped repository.
func NewExecutorGroupRepository(db *gorm.DB) (ExecutorGroupRepository, error) {
	if db == nil {
		return nil, errors.New("executor group repository database is nil")
	}
	return &executorGroupRepository{db: db}, nil
}

func (r *executorGroupRepository) Create(ctx context.Context, group *model.ExecutorGroup) (int64, error) {
	if group == nil {
		return 0, errors.New("executor group is nil")
	}
	record := *group
	record.Base = basemodel.Base{}
	if err := r.db.WithContext(ctx).Create(&record).Error; err != nil {
		return 0, mapExecutorGroupWriteError(err)
	}
	group.Base = record.Base
	return record.ID, nil
}

func (r *executorGroupRepository) FindByID(ctx context.Context, id int64) (*model.ExecutorGroup, error) {
	var group model.ExecutorGroup
	err := r.db.WithContext(ctx).Where("id = ?", id).Take(&group).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, ErrExecutorGroupUnavailable
	}
	if err != nil {
		return nil, fmt.Errorf("find executor group: %w", err)
	}
	return &group, nil
}

func (r *executorGroupRepository) Delete(ctx context.Context, id int64) error {
	result := r.db.WithContext(ctx).Where("id = ?", id).Delete(&model.ExecutorGroup{})
	if result.Error != nil {
		return fmt.Errorf("delete executor group: %w", result.Error)
	}
	if result.RowsAffected == 0 {
		return ErrExecutorGroupUnavailable
	}
	if result.RowsAffected != 1 {
		return fmt.Errorf("executor group deletion affected %d rows", result.RowsAffected)
	}
	return nil
}

func (r *executorGroupRepository) List(ctx context.Context, f ExecutorGroupFilter) ([]ExecutorGroupListItem, int64, error) {
	if f.Page < 1 || f.PageSize < 1 || f.PageSize > 200 || f.Page-1 > (1<<63-1)/f.PageSize {
		return nil, 0, errors.New("invalid executor group pagination")
	}
	db := r.db.WithContext(ctx).Model(&model.ExecutorGroup{})
	if f.Name != "" {
		db = db.Where("name ILIKE ? ESCAPE '!'", literalContains(f.Name))
	}
	var total int64
	if err := db.Count(&total).Error; err != nil {
		return nil, 0, fmt.Errorf("count executor groups: %w", err)
	}
	items := make([]ExecutorGroupListItem, 0)
	if err := db.Select("id", "name", "discovery_key", "created_at", "updated_at").
		Order("id DESC").Limit(int(f.PageSize)).Offset(int((f.Page - 1) * f.PageSize)).Find(&items).Error; err != nil {
		return nil, 0, fmt.Errorf("list executor groups: %w", err)
	}
	return items, total, nil
}

func (r *executorGroupRepository) LoadExecutionGroups(ctx context.Context) ([]ExecutorGroupConfig, error) {
	items := make([]ExecutorGroupConfig, 0)
	if err := r.db.WithContext(ctx).Model(&model.ExecutorGroup{}).
		Select("id", "name", "discovery_key", "token").Find(&items).Error; err != nil {
		return nil, fmt.Errorf("load executor groups: %w", err)
	}
	return items, nil
}

func (r *executorGroupRepository) FindExecutionGroupByID(ctx context.Context, id int64) (*ExecutorGroupConfig, error) {
	return findExecutionGroup(r.db.WithContext(ctx).Where("id = ?", id))
}

func (r *executorGroupRepository) FindExecutionGroupByName(ctx context.Context, name string) (*ExecutorGroupConfig, error) {
	return findExecutionGroup(r.db.WithContext(ctx).Where("name = ?", name))
}

func findExecutionGroup(db *gorm.DB) (*ExecutorGroupConfig, error) {
	var group ExecutorGroupConfig
	err := db.Model(&model.ExecutorGroup{}).Select("id", "name", "discovery_key", "token").Take(&group).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, ErrExecutorGroupUnavailable
	}
	if err != nil {
		return nil, fmt.Errorf("find executor group configuration: %w", err)
	}
	return &group, nil
}

func mapExecutorGroupWriteError(err error) error {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "23505" {
		switch pgErr.ConstraintName {
		case "uq_job_executor_group_name":
			return ErrExecutorGroupNameExists
		case "uq_job_executor_group_discovery_key":
			return ErrExecutorGroupDiscoveryKeyExists
		}
	}
	return fmt.Errorf("create executor group: %w", err)
}

func (r *executorGroupRepository) LockAvailable(ctx context.Context, id int64) error {
	// The caller holds an explicit transaction. This primary-key lock prevents
	// a group deletion from racing a new or changed Job reference (group -> Job).
	var group model.ExecutorGroup
	err := r.db.WithContext(ctx).Unscoped().Select("id", "deleted_at").
		Clauses(clause.Locking{Strength: "UPDATE"}).Where("id = ?", id).Take(&group).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return ErrExecutorGroupUnavailable
	}
	if err != nil {
		return fmt.Errorf("lock executor group: %w", err)
	}
	if group.DeletedAt.Valid {
		return ErrExecutorGroupUnavailable
	}
	return nil
}

func (r *executorGroupRepository) Available(ctx context.Context, id int64) (bool, error) {
	var available bool
	db := r.db.WithContext(ctx)
	query := db.Model(&model.ExecutorGroup{}).Select("1").Where("id = ?", id)
	// GORM query builders require an outer FROM; keep only the tableless EXISTS
	// wrapper as SQL while the model subquery applies the normal soft-delete scope.
	if err := db.Raw("SELECT EXISTS (?)", query).Scan(&available).Error; err != nil {
		return false, fmt.Errorf("check executor group availability: %w", err)
	}
	return available, nil
}
