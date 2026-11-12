package instancecheck

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/tokyolab/dogx/apps/job/internal/model"
	"github.com/tokyolab/dogx/apps/job/internal/repository"
	"github.com/zeromicro/go-zero/core/logx"
)

// PostCreator creates the singleton business Post from stable shard counts. It
// owns no connection, queue or goroutine. The caller must have confirmed successful
// preparation, a complete shard set or explicit empty preparation, and all shard
// outcomes as succeeded or finally failed outside the parent transaction.
type PostCreator struct {
	transact repository.TransactionRunner
	hint     func(int64)
}

// NewPostCreator binds an independently committing transaction runner and an
// optional nonblocking dispatch hint. Dependencies and hints must support concurrent
// calls. The host owns their lifetime; construction starts no work or database I/O.
func NewPostCreator(transact repository.TransactionRunner, hint func(int64)) (*PostCreator, error) {
	if transact == nil {
		return nil, errors.New("Post creation transaction runner is nil")
	}
	return &PostCreator{transact: transact, hint: hint}, nil
}

// Create saves one waiting Post and its immutable input under the parent's NOWAIT
// lock. Counts must satisfy total=succeeded+failed with no canceled shards; zero
// is valid only after the caller established an explicit empty preparation list.
// The transaction rechecks the frozen mode/Post setting, parent status and fresh
// database time, then checks Post existence. It never reaggregates shard results.
//
// True, nil confirms the independent commit and precedes an optional dispatch
// hint. False, nil means no creation and makes no claim about parent completion.
// Any error, including an uncertain commit, returns false without a hint. No
// result authorizes direct RPC, a new business attempt or immediate retry. The
// existing caller Context covers the entire operation without a renewed budget.
func (p *PostCreator) Create(ctx context.Context, instanceID int64, counts repository.ShardCounts) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if instanceID <= 0 || !validShardCounts(counts) || counts.Canceled != 0 || counts.Total-counts.Succeeded != counts.Failed {
		return false, ErrInvalidFacts
	}
	created := false
	err := p.transact(ctx, func(repos repository.Repositories) error {
		// Serialize authorization and singleton insertion with competing creation
		// and stop decisions, without holding the parent while reading shard facts.
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
		var config struct {
			SchemaVersion *int32               `json:"schema_version"`
			ExecutionMode *model.ExecutionMode `json:"execution_mode"`
			PostEnabled   *bool                `json:"post_enabled"`
		}
		if err := json.Unmarshal(parent.ExecutionConfig, &config); err != nil || config.SchemaVersion == nil || *config.SchemaVersion != 1 ||
			config.ExecutionMode == nil || *config.ExecutionMode != model.ExecutionModeSharded || config.PostEnabled == nil || !*config.PostEnabled {
			return ErrInvalidConfig
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
		post, err := repos.Tasks.FindPost(ctx, instanceID)
		if err != nil {
			return err
		}
		if post != nil {
			if !validStage(post, model.TaskTypePost) {
				return ErrInvalidFacts
			}
			return nil
		}
		id, err := repos.Tasks.InsertPost(ctx, instanceID, counts, now)
		if err != nil {
			return err
		}
		if id <= 0 {
			return ErrInvalidFacts
		}
		created = true
		return nil
	})
	if err != nil {
		return false, fmt.Errorf("create instance Post: %w", err)
	}
	if created && p.hint != nil {
		p.hintDispatch(instanceID)
	}
	return created, nil
}

func (p *PostCreator) hintDispatch(id int64) {
	defer func() {
		if recover() != nil {
			logx.Errorw("job Post dispatch handoff failed", logx.Field("instance_id", id))
		}
	}()
	p.hint(id)
}
