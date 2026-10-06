package version_test

import (
	"errors"
	"testing"

	"github.com/feng111627/pipeline-lineage-orchestrator-go/internal/version"
)

func open(t *testing.T) *version.Store {
	t.Helper()
	s, err := version.Open(t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	return s
}

// 场景：版本幂等 —— 同一份输入重复产出得到同一版本，不冒重复版本。
func TestProduce_Idempotent(t *testing.T) {
	t.Log("业务场景：版本幂等 —— 相同指纹重复 Produce 不产生新版本")
	s := open(t)
	v1, err := s.Produce("orders", "fp-1", []byte("data-v1"))
	if err != nil {
		t.Fatalf("produce: %v", err)
	}
	v2, err := s.Produce("orders", "fp-1", []byte("data-v1-again"))
	if err != nil {
		t.Fatalf("produce again: %v", err)
	}
	if v1.Seq != v2.Seq {
		t.Errorf("same fingerprint produced seq %d and %d, want identical", v1.Seq, v2.Seq)
	}
	versions, _ := s.List("orders")
	if len(versions) != 1 {
		t.Errorf("version count = %d, want 1 (no duplicate)", len(versions))
	}
	// 内容保持首次写入，重复执行结果一致
	got, _ := s.Get("orders", v1.Seq)
	if string(got) != "data-v1" {
		t.Errorf("content = %q, want first-write content", got)
	}
	// 不同指纹才产生新版本
	v3, _ := s.Produce("orders", "fp-2", []byte("data-v2"))
	if v3.Seq != 2 {
		t.Errorf("new fingerprint seq = %d, want 2", v3.Seq)
	}
	t.Logf("结果：同指纹复用 v%d，版本总数=%d；新指纹产出 v%d", v1.Seq, len(versions), v3.Seq)
}

// 场景：回滚 —— 回滚到历史版本后读取内容与当时完全一致。
func TestRollback_ConsistentRead(t *testing.T) {
	t.Log("业务场景：回滚 —— 回滚后查询结果与历史版本完全一致")
	s := open(t)
	v1, _ := s.Produce("orders", "fp-1", []byte("data-v1"))
	v2, _ := s.Produce("orders", "fp-2", []byte("data-v2"))
	// 新产出的 v2 已自动生效；晋升只是打发布标记
	if _, err := s.Promote("orders", v2.Seq, v2.Seq); err != nil {
		t.Fatalf("promote v2: %v", err)
	}
	cur, _ := s.Current("orders")
	if cur.Seq != v2.Seq || !cur.Promoted {
		t.Fatalf("current = %+v, want seq=%d promoted", cur, v2.Seq)
	}
	// 回滚到 v1
	if _, err := s.Rollback("orders", v1.Seq, v2.Seq); err != nil {
		t.Fatalf("rollback: %v", err)
	}
	cur, _ = s.Current("orders")
	if cur.Seq != v1.Seq {
		t.Fatalf("current = %d, want %d after rollback", cur.Seq, v1.Seq)
	}
	got, _ := s.Get("orders", cur.Seq)
	if string(got) != "data-v1" {
		t.Errorf("content after rollback = %q, want %q", got, "data-v1")
	}
	t.Logf("结果：v%d 晋升后回滚到 v%d，读取内容=%q 与当时一致", v2.Seq, v1.Seq, got)
}

// 场景：晋升/回滚冲突 —— 期望版本不匹配时明确报错，不做默默覆盖。
func TestPromoteRollback_Conflict(t *testing.T) {
	t.Log("业务场景：冲突报错 —— expectCurrent 不匹配时拒绝修改")
	s := open(t)
	v1, _ := s.Produce("orders", "fp-1", []byte("d1"))
	v2, _ := s.Produce("orders", "fp-2", []byte("d2"))
	// 产出 v2 后当前版本已是 v2

	// 错误的 expectCurrent：晋升必须报冲突
	if _, err := s.Promote("orders", v2.Seq, 99); !errors.Is(err, version.ErrConflict) {
		t.Fatalf("promote err = %v, want ErrConflict", err)
	}
	// 冲突后状态未被修改
	cur, _ := s.Current("orders")
	if cur.Seq != v2.Seq {
		t.Errorf("current changed to %d after conflicting promote", cur.Seq)
	}
	// 正常晋升后再用旧 expectCurrent 回滚：同样冲突
	if _, err := s.Promote("orders", v2.Seq, v2.Seq); err != nil {
		t.Fatalf("promote: %v", err)
	}
	if _, err := s.Rollback("orders", v1.Seq, v1.Seq); !errors.Is(err, version.ErrConflict) {
		t.Fatalf("rollback err = %v, want ErrConflict", err)
	}
	cur, _ = s.Current("orders")
	if cur.Seq != v2.Seq {
		t.Errorf("current changed to %d after conflicting rollback", cur.Seq)
	}
	t.Logf("结果：两次冲突操作均被拒绝且状态保持 v%d，无默默覆盖", cur.Seq)
}

// 场景：不存在的数据集/版本 —— 语义化错误而非底层文件错误。
func TestGet_NotFound(t *testing.T) {
	t.Log("业务场景：错误语义化 —— 查询不存在的版本返回 ErrNotFound")
	s := open(t)
	if _, err := s.Get("ghost", 1); !errors.Is(err, version.ErrNotFound) {
		t.Errorf("err = %v, want ErrNotFound", err)
	}
	if _, err := s.Current("ghost"); !errors.Is(err, version.ErrNotFound) {
		t.Errorf("err = %v, want ErrNotFound", err)
	}
	t.Log("结果：均返回 ErrNotFound，未透出底层文件错误")
}
