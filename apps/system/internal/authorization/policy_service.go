package authorization

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"

	gormadapter "github.com/casbin/gorm-adapter/v3"
	"github.com/tokyolab/dogx/apps/system/internal/model"
	"github.com/tokyolab/dogx/apps/system/internal/repository"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var (
	ErrRoleUnavailable = errors.New("role does not exist or is disabled")
	ErrAPIUnavailable  = errors.New("API does not exist or is disabled")
)

type ReplaceResult struct {
	Added             int
	Removed           int
	NotificationError error
}

type UserSessionRevoker interface {
	RevokeAll(ctx context.Context, userID int64) error
}

type DeleteRoleResult struct {
	RemovedPolicies   int
	RevokedUsers      int
	NotificationError error
}

func (r ReplaceResult) Changed() bool {
	return r.Added > 0 || r.Removed > 0
}

type RolePolicyService struct {
	db       *gorm.DB
	adapter  *gormadapter.Adapter
	notifier PolicyNotifier
}

func NewRolePolicyService(db *gorm.DB, notifier PolicyNotifier) (*RolePolicyService, error) {
	if notifier == nil {
		return nil, errors.New("policy notifier is nil")
	}
	adapter, err := NewGormAdapter(db)
	if err != nil {
		return nil, err
	}
	return &RolePolicyService{db: db, adapter: adapter, notifier: notifier}, nil
}

func (s *RolePolicyService) ListRoleAPIIDs(ctx context.Context, roleID int64) ([]int64, error) {
	if ctx == nil {
		return nil, errors.New("list role API ids context is nil")
	}
	subject, err := RoleSubject(roleID)
	if err != nil {
		return nil, err
	}

	apiIDs := make([]int64, 0)
	if err := s.db.WithContext(ctx).
		Model(&model.API{}).
		Distinct("sys_api.id").
		Joins(`
			JOIN casbin_rule
			  ON casbin_rule.ptype = 'p'
			 AND casbin_rule.v0 = ?
			 AND casbin_rule.v1 = sys_api.path
			 AND casbin_rule.v2 = sys_api.method
		`, subject).
		Order("sys_api.id ASC").
		Pluck("sys_api.id", &apiIDs).Error; err != nil {
		return nil, fmt.Errorf("list role API ids: %w", err)
	}
	return apiIDs, nil
}

func (s *RolePolicyService) ReplaceRoleAPIs(
	ctx context.Context,
	roleID int64,
	apiIDs []int64,
) (ReplaceResult, error) {
	if ctx == nil {
		return ReplaceResult{}, errors.New("replace role APIs context is nil")
	}
	if roleID <= 0 {
		return ReplaceResult{}, ErrInvalidRoleID
	}
	targetIDs, err := normalizedIDs(apiIDs)
	if err != nil {
		return ReplaceResult{}, err
	}

	tx, err := s.adapter.BeginTransaction(ctx)
	if err != nil {
		return ReplaceResult{}, fmt.Errorf("begin role policy transaction: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	txAdapter, ok := tx.GetAdapter().(*gormadapter.Adapter)
	if !ok {
		return ReplaceResult{}, errors.New("unexpected transactional Casbin adapter")
	}
	txDB := newTransactionQueryDB(txAdapter.GetDb(), ctx)
	lockName := "dogx:casbin:role:" + strconv.FormatInt(roleID, 10)
	if err := txDB.Exec("SELECT pg_advisory_xact_lock(hashtextextended(?, 0))", lockName).Error; err != nil {
		return ReplaceResult{}, fmt.Errorf("lock role policy: %w", err)
	}

	resources, err := loadTargetResources(ctx, txDB, roleID, targetIDs)
	if err != nil {
		return ReplaceResult{}, err
	}
	current, err := loadCurrentRules(txAdapter, roleID)
	if err != nil {
		return ReplaceResult{}, err
	}
	target, err := resourceRules(roleID, resources)
	if err != nil {
		return ReplaceResult{}, err
	}

	removed, added := policyDifference(current, target)
	if len(removed) > 0 {
		if err := txAdapter.RemovePoliciesCtx(ctx, "p", "p", removed); err != nil {
			return ReplaceResult{}, fmt.Errorf("remove role policies: %w", err)
		}
	}
	if len(added) > 0 {
		if err := txAdapter.AddPoliciesCtx(ctx, "p", "p", added); err != nil {
			return ReplaceResult{}, fmt.Errorf("add role policies: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return ReplaceResult{}, fmt.Errorf("commit role policy transaction: %w", err)
	}
	committed = true

	result := ReplaceResult{Added: len(added), Removed: len(removed)}
	if result.Changed() {
		result.NotificationError = s.notifier.Update()
	}
	return result, nil
}

func (s *RolePolicyService) UpdateRoleStatus(
	ctx context.Context,
	roleID int64,
	roleStatus model.RecordStatus,
	sessions UserSessionRevoker,
) error {
	if ctx == nil {
		return errors.New("update role status context is nil")
	}
	if roleID <= 0 ||
		(roleStatus != model.RecordStatusDisabled && roleStatus != model.RecordStatusEnabled) {
		return ErrInvalidRoleID
	}
	if sessions == nil {
		return errors.New("user session revoker is nil")
	}

	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var role model.Role
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&role, roleID).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return repository.ErrRoleNotFound
			}
			return fmt.Errorf("lock role for status update: %w", err)
		}
		if role.IsSystem && roleStatus == model.RecordStatusDisabled {
			return repository.ErrSystemRoleProtected
		}
		if role.Status == roleStatus {
			return nil
		}

		if roleStatus == model.RecordStatusDisabled {
			if _, err := revokeRoleUserSessions(ctx, tx, roleID, sessions); err != nil {
				return err
			}
		}
		if err := tx.Model(&role).Update("status", roleStatus).Error; err != nil {
			return fmt.Errorf("update role status: %w", err)
		}
		return nil
	})
}

func (s *RolePolicyService) DeleteRole(
	ctx context.Context,
	roleID int64,
	sessions UserSessionRevoker,
) (DeleteRoleResult, error) {
	if ctx == nil {
		return DeleteRoleResult{}, errors.New("delete role context is nil")
	}
	if roleID <= 0 {
		return DeleteRoleResult{}, ErrInvalidRoleID
	}
	if sessions == nil {
		return DeleteRoleResult{}, errors.New("user session revoker is nil")
	}

	tx, err := s.adapter.BeginTransaction(ctx)
	if err != nil {
		return DeleteRoleResult{}, fmt.Errorf("begin role deletion transaction: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	txAdapter, ok := tx.GetAdapter().(*gormadapter.Adapter)
	if !ok {
		return DeleteRoleResult{}, errors.New("unexpected transactional Casbin adapter")
	}
	txDB := newTransactionQueryDB(txAdapter.GetDb(), ctx)
	lockName := "dogx:casbin:role:" + strconv.FormatInt(roleID, 10)
	if err := txDB.Exec("SELECT pg_advisory_xact_lock(hashtextextended(?, 0))", lockName).Error; err != nil {
		return DeleteRoleResult{}, fmt.Errorf("lock role deletion: %w", err)
	}

	var role model.Role
	if err := txDB.Clauses(clause.Locking{Strength: "UPDATE"}).First(&role, roleID).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return DeleteRoleResult{}, repository.ErrRoleNotFound
		}
		return DeleteRoleResult{}, fmt.Errorf("load role for deletion: %w", err)
	}
	if role.IsSystem {
		return DeleteRoleResult{}, repository.ErrSystemRoleProtected
	}

	revokedUsers, err := revokeRoleUserSessions(ctx, txDB, roleID, sessions)
	if err != nil {
		return DeleteRoleResult{}, err
	}
	current, err := loadCurrentRules(txAdapter, roleID)
	if err != nil {
		return DeleteRoleResult{}, err
	}
	if len(current) > 0 {
		if err := txAdapter.RemovePoliciesCtx(ctx, "p", "p", current); err != nil {
			return DeleteRoleResult{}, fmt.Errorf("remove deleted role policies: %w", err)
		}
	}
	if err := txDB.Where("role_id = ?", roleID).Delete(&model.RoleMenu{}).Error; err != nil {
		return DeleteRoleResult{}, fmt.Errorf("delete role menu assignments: %w", err)
	}
	if err := txDB.Where("role_id = ?", roleID).Delete(&model.UserRole{}).Error; err != nil {
		return DeleteRoleResult{}, fmt.Errorf("delete user role assignments: %w", err)
	}
	if err := txDB.Delete(&role).Error; err != nil {
		return DeleteRoleResult{}, fmt.Errorf("soft delete role: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return DeleteRoleResult{}, fmt.Errorf("commit role deletion transaction: %w", err)
	}
	committed = true

	result := DeleteRoleResult{
		RemovedPolicies: len(current),
		RevokedUsers:    revokedUsers,
	}
	if result.RemovedPolicies > 0 {
		result.NotificationError = s.notifier.Update()
	}
	return result, nil
}

func revokeRoleUserSessions(
	ctx context.Context,
	db *gorm.DB,
	roleID int64,
	sessions UserSessionRevoker,
) (int, error) {
	userIDs := make([]int64, 0)
	if err := db.WithContext(ctx).
		Model(&model.UserRole{}).
		Where("role_id = ?", roleID).
		Order("user_id ASC").
		Pluck("user_id", &userIDs).Error; err != nil {
		return 0, fmt.Errorf("list role users for session revocation: %w", err)
	}
	for _, userID := range userIDs {
		if err := sessions.RevokeAll(ctx, userID); err != nil {
			return 0, fmt.Errorf("revoke sessions for role user %d: %w", userID, err)
		}
	}
	return len(userIDs), nil
}

// newTransactionQueryDB keeps the Adapter transaction connection while
// discarding its casbin_rule table scope so GORM can select business tables
// from their models.
func newTransactionQueryDB(adapterDB *gorm.DB, ctx context.Context) *gorm.DB {
	return adapterDB.Session(&gorm.Session{
		Context:     ctx,
		Initialized: true,
		NewDB:       true,
	})
}

func loadTargetResources(
	ctx context.Context,
	db *gorm.DB,
	roleID int64,
	targetIDs []int64,
) ([]model.API, error) {
	var role model.Role
	if err := db.WithContext(ctx).
		Where("id = ? AND status = ?", roleID, model.RecordStatusEnabled).
		First(&role).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrRoleUnavailable
		}
		return nil, fmt.Errorf("load role for policy update: %w", err)
	}

	if len(targetIDs) > 0 {
		var count int64
		if err := db.WithContext(ctx).
			Model(&model.API{}).
			Where("id IN ? AND status = ?", targetIDs, model.RecordStatusEnabled).
			Count(&count).Error; err != nil {
			return nil, fmt.Errorf("validate role API resources: %w", err)
		}
		if count != int64(len(targetIDs)) {
			return nil, ErrAPIUnavailable
		}
	}

	query := db.WithContext(ctx).
		Where("status = ?", model.RecordStatusEnabled)
	if len(targetIDs) == 0 {
		query = query.Where("is_required = TRUE")
	} else {
		query = query.Where("is_required = TRUE OR id IN ?", targetIDs)
	}
	var resources []model.API
	if err := query.Order("path, method, id").Find(&resources).Error; err != nil {
		return nil, fmt.Errorf("load role API resources: %w", err)
	}
	return resources, nil
}

func loadCurrentRules(adapter *gormadapter.Adapter, roleID int64) ([][]string, error) {
	policy, err := NewModel()
	if err != nil {
		return nil, err
	}
	subject, err := RoleSubject(roleID)
	if err != nil {
		return nil, err
	}
	if err := adapter.LoadFilteredPolicy(policy, gormadapter.Filter{
		Ptype: []string{"p"},
		V0:    []string{subject},
	}); err != nil {
		return nil, fmt.Errorf("load current role policies: %w", err)
	}
	rules, err := policy.GetPolicy("p", "p")
	if err != nil {
		return nil, fmt.Errorf("read current role policies: %w", err)
	}
	return rules, nil
}

func resourceRules(roleID int64, resources []model.API) ([][]string, error) {
	rules := make([][]string, 0, len(resources))
	for _, resource := range resources {
		rule, err := PolicyRule(roleID, resource.Path, resource.Method)
		if err != nil {
			return nil, fmt.Errorf("build policy for API %d: %w", resource.ID, err)
		}
		rules = append(rules, rule)
	}
	return rules, nil
}

func policyDifference(current, target [][]string) (removed, added [][]string) {
	currentSet := make(map[string][]string, len(current))
	targetSet := make(map[string][]string, len(target))
	for _, rule := range current {
		currentSet[policyKey(rule)] = rule
	}
	for _, rule := range target {
		targetSet[policyKey(rule)] = rule
	}
	for key, rule := range currentSet {
		if _, exists := targetSet[key]; !exists {
			removed = append(removed, rule)
		}
	}
	for key, rule := range targetSet {
		if _, exists := currentSet[key]; !exists {
			added = append(added, rule)
		}
	}
	sort.Slice(removed, func(i, j int) bool { return policyKey(removed[i]) < policyKey(removed[j]) })
	sort.Slice(added, func(i, j int) bool { return policyKey(added[i]) < policyKey(added[j]) })
	return removed, added
}

func policyKey(rule []string) string {
	return fmt.Sprintf("%q", rule)
}

func normalizedIDs(ids []int64) ([]int64, error) {
	unique := make(map[int64]struct{}, len(ids))
	for _, id := range ids {
		if id <= 0 {
			return nil, errors.New("API ids must be positive")
		}
		unique[id] = struct{}{}
	}
	result := make([]int64, 0, len(unique))
	for id := range unique {
		result = append(result, id)
	}
	sort.Slice(result, func(i, j int) bool { return result[i] < result[j] })
	return result, nil
}
