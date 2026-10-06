package version

import (
	"testing"

	"github.com/feng111627/pipeline-lineage-orchestrator-go/internal/errs"
)

func caseLog(t *testing.T, business, name, result string) {
	t.Helper()
	t.Logf("[业务] %s | 用例: %s | 结果: %s", business, name, result)
}

func openTemp(t *testing.T) *Store {
	t.Helper()
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("打开版本存储失败: %v", err)
	}
	return s
}

// TestCommitIdempotent 业务：版本幂等 —— 同一输入重复产出得到同一版本，不产生重复。
func TestCommitIdempotent(t *testing.T) {
	t.Log("【业务覆盖】版本幂等：同一产物+同一输入指纹+同一内容 => 同一版本号，不重复")
	s := openTemp(t)

	v1, created1, err := s.Commit("orders", "input-hash-1", []byte("data-v1"))
	if err != nil || !created1 {
		t.Fatalf("首次产出应创建新版本: created=%v err=%v", created1, err)
	}
	v2, created2, err := s.Commit("orders", "input-hash-1", []byte("data-v1"))
	if err != nil {
		t.Fatalf("重复产出不应报错: %v", err)
	}
	if created2 {
		t.Fatalf("重复产出不应创建新版本")
	}
	if v1.ID != v2.ID {
		t.Fatalf("同一输入应得到同一版本号: %s != %s", v1.ID, v2.ID)
	}
	caseLog(t, "版本幂等", "同一输入重复产出返回同一版本号", "通过")
}

// TestCommitConflict 业务：版本幂等 —— 同一输入指纹内容不一致时报冲突而非覆盖。
func TestCommitConflict(t *testing.T) {
	t.Log("【业务覆盖】版本幂等：同一输入指纹内容不一致 => 明确冲突错误，不默默覆盖")
	s := openTemp(t)

	if _, _, err := s.Commit("orders", "input-hash-1", []byte("data-v1")); err != nil {
		t.Fatalf("首次产出失败: %v", err)
	}
	_, _, err := s.Commit("orders", "input-hash-1", []byte("data-DIFFERENT"))
	if err == nil {
		t.Fatalf("内容不一致应报冲突")
	}
	if !errs.IsKind(err, errs.KindConflict) {
		t.Fatalf("应分类为 conflict，实际: %v", err)
	}
	// 原版本数据未被覆盖
	v, gerr := s.Get("orders", versionID("input-hash-1", hashOf([]byte("data-v1"))))
	if gerr != nil || string(v.Data) != "data-v1" {
		t.Fatalf("原版本数据被覆盖: v=%v err=%v", v, gerr)
	}
	caseLog(t, "版本幂等", "同指纹不同内容报冲突且原版本未被覆盖", "通过")
}

// TestPromoteConflict 业务：版本晋升 —— 乐观并发，期望不符时报冲突。
func TestPromoteConflict(t *testing.T) {
	t.Log("【业务覆盖】版本晋升：通道被其他版本占用且期望不符 => 明确冲突错误")
	s := openTemp(t)

	v1, _, _ := s.Commit("orders", "ih-1", []byte("d1"))
	v2, _, _ := s.Commit("orders", "ih-2", []byte("d2"))

	if err := s.Promote("orders", "current", v1.ID, ""); err != nil {
		t.Fatalf("首次晋升应成功: %v", err)
	}
	// 期望不符 => 冲突
	err := s.Promote("orders", "current", v2.ID, "v-不存在的期望")
	if !errs.IsKind(err, errs.KindConflict) {
		t.Fatalf("期望不符应报 conflict，实际: %v", err)
	}
	// 期望相符 => 成功
	if err := s.Promote("orders", "current", v2.ID, v1.ID); err != nil {
		t.Fatalf("期望相符的晋升应成功: %v", err)
	}
	cur, err := s.Current("orders", "current")
	if err != nil || cur.ID != v2.ID {
		t.Fatalf("通道应指向 v2: cur=%v err=%v", cur, err)
	}
	// 幂等晋升：重复晋升同一版本不报错
	if err := s.Promote("orders", "current", v2.ID, v2.ID); err != nil {
		t.Fatalf("幂等晋升不应报错: %v", err)
	}
	caseLog(t, "版本晋升", "期望不符冲突/期望相符成功/幂等晋升", "通过")
}

// TestRollback 业务：版本回滚 —— 回滚后查询与历史版本完全一致。
func TestRollback(t *testing.T) {
	t.Log("【业务覆盖】版本回滚：回滚到历史版本后查询结果与当时完全一致")
	s := openTemp(t)

	v1, _, _ := s.Commit("orders", "ih-1", []byte("data-v1"))
	v2, _, _ := s.Commit("orders", "ih-2", []byte("data-v2"))
	if err := s.Promote("orders", "current", v1.ID, ""); err != nil {
		t.Fatalf("晋升 v1 失败: %v", err)
	}
	if err := s.Promote("orders", "current", v2.ID, v1.ID); err != nil {
		t.Fatalf("晋升 v2 失败: %v", err)
	}

	// 回滚到 v1，期望当前为 v2
	if err := s.Rollback("orders", "current", v1.ID, v2.ID); err != nil {
		t.Fatalf("回滚失败: %v", err)
	}
	cur, err := s.Current("orders", "current")
	if err != nil {
		t.Fatalf("回滚后查询失败: %v", err)
	}
	if cur.ID != v1.ID || string(cur.Data) != "data-v1" || cur.DataHash != v1.DataHash {
		t.Fatalf("回滚后内容与历史版本不一致: %+v", cur)
	}
	caseLog(t, "版本回滚", "回滚到 v1 后查询结果与 v1 当时完全一致", "通过")
}

// TestRollbackConflict 业务：版本回滚 —— 期望当前版本不符时明确冲突，不默默覆盖。
func TestRollbackConflict(t *testing.T) {
	t.Log("【业务覆盖】版本回滚：期望当前版本与实际不符 => 明确冲突错误，通道不变")
	s := openTemp(t)

	v1, _, _ := s.Commit("orders", "ih-1", []byte("data-v1"))
	v2, _, _ := s.Commit("orders", "ih-2", []byte("data-v2"))
	if err := s.Promote("orders", "current", v2.ID, ""); err != nil {
		t.Fatalf("晋升失败: %v", err)
	}

	err := s.Rollback("orders", "current", v1.ID, "v-错误期望")
	if !errs.IsKind(err, errs.KindConflict) {
		t.Fatalf("期望不符应报 conflict，实际: %v", err)
	}
	cur, _ := s.Current("orders", "current")
	if cur.ID != v2.ID {
		t.Fatalf("冲突回滚不应改变通道指向，当前 %s", cur.ID)
	}
	// 回滚到不存在的版本 => not_found
	err = s.Rollback("orders", "current", "v-不存在", v2.ID)
	if !errs.IsKind(err, errs.KindNotFound) {
		t.Fatalf("回滚到不存在版本应报 not_found，实际: %v", err)
	}
	caseLog(t, "版本回滚", "期望不符冲突且通道不变/目标版本不存在报 not_found", "通过")
}
