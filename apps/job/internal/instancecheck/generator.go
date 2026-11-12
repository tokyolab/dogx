package instancecheck

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/tokyolab/dogx/apps/job/internal/model"
	"github.com/tokyolab/dogx/apps/job/internal/repository"
	"github.com/tokyolab/dogx/pkg/jobexecutor"
	"github.com/zeromicro/go-zero/core/logx"
)

// Generator creates the complete shard set from one durably saved preparation
// result. It owns no queue, connection or retry loop. Concurrent callers coordinate
// through the same parent row; the host owns the transaction runner and callback.
type Generator struct {
	transact repository.TransactionRunner
	hint     func(int64)
}

// NewGenerator binds an independently committing transaction runner and an optional
// dispatch hint. The hint must be concurrency-safe and nonblocking, without I/O;
// it receives no processing Context. Losing it cannot lose the persisted work.
func NewGenerator(transact repository.TransactionRunner, hint func(int64)) (*Generator, error) {
	if transact == nil {
		return nil, errors.New("shard generation transaction runner is nil")
	}
	return &Generator{transact: transact, hint: hint}, nil
}

// Generate parses a saved, successful preparation result before opening a short
// transaction. The caller must obtain prepared from FindPreparationResult, only
// after the minimal preparation and shard-existence reads establish that generation
// is needed; an uncommitted executor report must never be supplied here. The
// terminal preparation result is immutable and its identity and success are
// rechecked under the parent's NOWAIT lock before any insertion.
// The caller must not modify prepared.Data until Generate returns.
//
// A nonempty list is inserted atomically, one complete JSON value per Task, only
// while the parent is running in sharded mode and before its original deadline.
// Existing shards, an inactive or missing parent, an expired deadline, or an
// explicit empty list return false, nil. Empty lists need no generation transaction;
// the caller separately decides Post creation or normal completion. Invalid saved
// facts return ErrInvalidFacts; unusable frozen settings return ErrInvalidConfig.
// Lock conflicts preserve repository.ErrInstanceLocked and are not retried here.
//
// True, nil confirms creation and commit of the entire set, followed by an optional
// dispatch hint. Any error returns false, including an uncertain commit; no error
// authorizes sending or immediate requeueing. The caller's shared check budget
// covers parsing, database work and cleanup, without a fresh per-step deadline.
// This operation never marks the parent complete or hints another completion check.
func (g *Generator) Generate(ctx context.Context, instanceID int64, prepared repository.PreparationResult) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if instanceID <= 0 || prepared.ID <= 0 || prepared.Status != model.TaskSucceeded {
		return false, ErrInvalidFacts
	}
	items, err := preparationItems(prepared.Data)
	if err != nil {
		return false, err
	}
	if len(items) == 0 {
		return false, nil
	}
	created := false
	err = g.transact(ctx, func(repos repository.Repositories) error {
		// The parent lock excludes competing generation and stop decisions. The
		// existence check must remain inside it: a prior unlocked read is only a hint.
		parent, err := repos.Instances.LockForExecution(ctx, instanceID)
		if err != nil {
			return err
		}
		if parent == nil {
			return nil
		}
		if parent.ID != instanceID {
			return ErrInvalidFacts
		}
		switch parent.Status {
		case model.InstanceRunning:
		case model.InstanceWaiting, model.InstanceStopping, model.InstanceSucceeded, model.InstanceFailed, model.InstanceCanceled:
			return nil
		default:
			return ErrInvalidFacts
		}
		now, err := repos.Instances.DatabaseTime(ctx)
		if err != nil {
			return err
		}
		if now.IsZero() || parent.StartedAt == nil || parent.StartedAt.IsZero() || parent.DeadlineAt == nil ||
			!parent.DeadlineAt.After(*parent.StartedAt) || parent.FinishedAt != nil {
			return ErrInvalidFacts
		}
		if !now.Before(*parent.DeadlineAt) {
			return nil
		}
		var config struct {
			SchemaVersion *int32               `json:"schema_version"`
			ExecutionMode *model.ExecutionMode `json:"execution_mode"`
			PostEnabled   *bool                `json:"post_enabled"`
		}
		if err := json.Unmarshal(parent.ExecutionConfig, &config); err != nil || config.SchemaVersion == nil ||
			*config.SchemaVersion != 1 || config.ExecutionMode == nil || *config.ExecutionMode != model.ExecutionModeSharded || config.PostEnabled == nil {
			return ErrInvalidConfig
		}
		current, err := repos.Tasks.FindPreparation(ctx, instanceID)
		if err != nil {
			return err
		}
		if current == nil || current.ID != prepared.ID || current.Status != model.TaskSucceeded {
			return ErrInvalidFacts
		}
		exists, err := repos.Tasks.HasShards(ctx, instanceID)
		if err != nil || exists {
			return err
		}
		if err := repos.Tasks.InsertShards(ctx, instanceID, items, now); err != nil {
			return err
		}
		created = true
		return nil
	})
	if err != nil {
		return false, fmt.Errorf("generate instance shards: %w", err)
	}
	if created && g.hint != nil {
		g.hintDispatch(instanceID)
	}
	return created, nil
}

func preparationItems(data model.TaskData) ([]model.TaskData, error) {
	var rawItems []json.RawMessage
	if err := json.Unmarshal(data, &rawItems); err != nil || rawItems == nil || len(rawItems) > jobexecutor.MaxPreparedItems {
		return nil, fmt.Errorf("%w: invalid saved preparation list", ErrInvalidFacts)
	}
	items := make([]model.TaskData, len(rawItems))
	for i := range rawItems {
		items[i] = model.TaskData(rawItems[i])
	}
	return items, nil
}

func (g *Generator) hintDispatch(id int64) {
	// A host callback cannot undo a confirmed commit. The ordinary due scan can
	// discover these waiting Tasks even when local hint admission fails.
	defer func() {
		if recover() != nil {
			logx.Errorw("job shard dispatch handoff failed", logx.Field("instance_id", id))
		}
	}()
	g.hint(id)
}
