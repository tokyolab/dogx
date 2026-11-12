package instancecheck

import (
	"context"
	"fmt"

	"github.com/tokyolab/dogx/apps/job/internal/model"
	"github.com/tokyolab/dogx/apps/job/internal/repository"
	"github.com/tokyolab/dogx/pkg/jobexecutor"
	"github.com/zeromicro/go-zero/core/logx"
)

func (c *Checker) checkSharded(ctx context.Context, parent *repository.InstanceCompletion, postEnabled bool) (Outcome, error) {
	stages, err := c.tasks.FindStages(ctx, parent.ID)
	if err != nil {
		return OutcomeSkipped, fmt.Errorf("read instance stages: %w", err)
	}
	if stages == nil {
		return OutcomeSkipped, ErrInvalidFacts
	}
	pre := stages.Preparation
	if pre == nil {
		return c.stageNotReady(ctx, parent, "missing_preparation")
	}
	if !validStage(pre, model.TaskTypePrepare) {
		return OutcomeSkipped, ErrInvalidFacts
	}
	switch pre.Status {
	case model.TaskFailed:
		return c.complete(ctx, parent.ID, model.InstanceFailed, pre.ResultSummary)
	case model.TaskSucceeded:
	case model.TaskCanceled:
		return c.stageNotReady(ctx, parent, "canceled_preparation")
	default:
		return c.notReady(ctx, parent)
	}
	post := stages.Post
	if post != nil {
		if !postEnabled || !validStage(post, model.TaskTypePost) {
			return OutcomeSkipped, ErrInvalidFacts
		}
		switch post.Status {
		case model.TaskSucceeded, model.TaskFailed:
		case model.TaskCanceled:
			return c.stageNotReady(ctx, parent, "canceled_post")
		default:
			// A pending Post already has fixed input from completed shards. Do not
			// repeatedly aggregate their results while business work is outstanding.
			return c.notReady(ctx, parent)
		}
	}
	exists, err := c.tasks.HasShards(ctx, parent.ID)
	if err != nil {
		return OutcomeSkipped, fmt.Errorf("check instance shards: %w", err)
	}
	var counts repository.ShardCounts
	if !exists {
		prepared, err := c.tasks.FindPreparationResult(ctx, parent.ID)
		if err != nil {
			return OutcomeSkipped, fmt.Errorf("read preparation result: %w", err)
		}
		if prepared == nil || prepared.ID != pre.ID || prepared.Status != model.TaskSucceeded {
			return OutcomeSkipped, ErrInvalidFacts
		}
		items, err := preparationItems(prepared.Data)
		if err != nil {
			return OutcomeSkipped, err
		}
		if len(items) != 0 {
			if post != nil {
				// A completed Post cannot legitimately precede its nonempty shard set.
				return OutcomeSkipped, ErrInvalidFacts
			}
			return c.advance(ctx, parent, func() (bool, error) { return c.generator.Generate(ctx, parent.ID, *prepared) })
		}
		// Only the committed explicit empty array proves zero shards. There is no
		// generation write, so this check may create Post or complete the parent.
	} else {
		unfinished, err := c.tasks.HasUnfinishedShards(ctx, parent.ID)
		if err != nil {
			return OutcomeSkipped, fmt.Errorf("check unfinished shards: %w", err)
		}
		if unfinished {
			return c.notReady(ctx, parent)
		}
		counts, err = c.tasks.CountShards(ctx, parent.ID)
		if err != nil {
			return OutcomeSkipped, fmt.Errorf("count completed shards: %w", err)
		}
		if !validShardCounts(counts) || counts.Total == 0 || counts.Total-counts.Succeeded-counts.Failed != counts.Canceled {
			return OutcomeSkipped, ErrInvalidFacts
		}
		if counts.Canceled != 0 {
			return c.stageNotReady(ctx, parent, "canceled_shards")
		}
	}
	if postEnabled && post == nil {
		return c.advance(ctx, parent, func() (bool, error) { return c.post.Create(ctx, parent.ID, counts) })
	}
	target := model.InstanceSucceeded
	if counts.Failed != 0 || post != nil && post.Status == model.TaskFailed {
		target = model.InstanceFailed
	}
	postResult := "not configured"
	if post != nil {
		postResult = "succeeded"
		if post.Status == model.TaskFailed {
			postResult = "failed"
		}
	}
	summary := fmt.Sprintf("Shards: total=%d, succeeded=%d, failed=%d; Post: %s", counts.Total, counts.Succeeded, counts.Failed, postResult)
	return c.complete(ctx, parent.ID, target, &summary)
}

// advance chooses timeout before authorizing new work, then invokes at most one
// stage operation. Its transaction rechecks the deadline after locking. False or
// an error cannot fall through to timeout: another node may already have advanced.
func (c *Checker) advance(ctx context.Context, parent *repository.InstanceCompletion, action func() (bool, error)) (Outcome, error) {
	if parent.DeadlineAt == nil || parent.DeadlineAt.IsZero() {
		return OutcomeSkipped, ErrInvalidFacts
	}
	if err := ctx.Err(); err != nil {
		return OutcomeSkipped, err
	}
	now, err := c.instances.DatabaseTime(ctx)
	if err != nil {
		return OutcomeSkipped, fmt.Errorf("read stage authorization time: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return OutcomeSkipped, err
	}
	if now.IsZero() {
		return OutcomeSkipped, ErrInvalidFacts
	}
	if !now.Before(*parent.DeadlineAt) {
		return c.requestTimeout(ctx, parent)
	}
	created, err := action()
	if err != nil {
		return OutcomeSkipped, fmt.Errorf("advance instance stage: %w", err)
	}
	if !created {
		return OutcomeNotApplied, nil
	}
	return OutcomeAdvanced, nil
}

func (c *Checker) stageNotReady(ctx context.Context, parent *repository.InstanceCompletion, reason string) (Outcome, error) {
	logx.WithContext(ctx).Errorw("running sharded instance has incomplete stage facts",
		logx.Field("instance_id", parent.ID), logx.Field("reason", reason))
	return c.notReady(ctx, parent)
}

func validStage(stage *repository.StageTask, taskType model.TaskType) bool {
	return stage.ID > 0 && stage.TaskType == taskType && stage.Status >= model.TaskWaiting && stage.Status <= model.TaskCanceled
}

func validShardCounts(counts repository.ShardCounts) bool {
	return counts.Total >= 0 && counts.Total <= jobexecutor.MaxPreparedItems && counts.Succeeded >= 0 &&
		counts.Failed >= 0 && counts.Canceled >= 0 && counts.Succeeded <= counts.Total &&
		counts.Failed <= counts.Total-counts.Succeeded && counts.Canceled <= counts.Total-counts.Succeeded-counts.Failed
}
