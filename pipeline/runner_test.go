package pipeline

import (
	"context"
	"errors"
	"testing"

	"github.com/feng111627/pipeline-lineage-orchestrator-go/internal/errs"
	"github.com/feng111627/pipeline-lineage-orchestrator-go/internal/scheduler"
)

func caseLog(t *testing.T, business, name, result string) {
	t.Helper()
	t.Logf("[业务] %s | 用例: %s | 结果: %s", business, name, result)
}

func openRunner(t *testing.T) *Runner {
	t.Helper()
	r, err := Open(t.TempDir(), "code-v1.0.0", scheduler.RetryPolicy{MaxAttempts: 2})
	if err != nil {
		t.Fatalf("打开编排器失败: %v", err)
	}
	return r
}

func okTask(name string, deps []string, config, output string) Task {
	return Task{
		Name:   name,
		Deps:   deps,
		Config: []byte(config),
		Run:    func(ctx context.Context) ([]byte, error) { return []byte(output), nil },
	}
}

// TestLineageQuery 业务：血缘查询 —— 产物可回溯到输入、配置与代码版本。
func TestLineageQuery(t *testing.T) {
	t.Log("【业务覆盖】血缘查询：产物版本可回溯到上游输入、配置哈希与代码版本")
	r := openRunner(t)
	tasks := []Task{
		okTask("extract", nil, "cfg-extract", "raw-data"),
		okTask("transform", []string{"extract"}, "cfg-transform", "clean-data"),
	}
	rep, err := r.Run(context.Background(), tasks)
	if err != nil || !rep.OK() {
		t.Fatalf("流水线应成功: rep=%+v err=%v", rep, err)
	}

	rec, err := r.Lineage().Of("transform", rep.Nodes["transform"].VersionID)
	if err != nil {
		t.Fatalf("血缘查询失败: %v", err)
	}
	wantInput := "extract@" + rep.Nodes["extract"].VersionID
	if len(rec.Inputs) != 1 || rec.Inputs[0] != wantInput {
		t.Fatalf("血缘输入应为 %q，实际 %v", wantInput, rec.Inputs)
	}
	if rec.CodeVersion != "code-v1.0.0" {
		t.Fatalf("血缘代码版本应为 code-v1.0.0，实际 %s", rec.CodeVersion)
	}
	if rec.ConfigHash == "" || rec.RunID != rep.RunID {
		t.Fatalf("血缘应包含配置哈希与运行ID: %+v", rec)
	}
	caseLog(t, "血缘查询", "transform 版本回溯到 extract 输入+配置哈希+代码版本", "通过")
}

// TestAuditTrail 业务：执行审计 —— 每次执行的成功失败、耗时与失败分类可查。
func TestAuditTrail(t *testing.T) {
	t.Log("【业务覆盖】执行审计：每次执行记录状态/耗时/失败分类，底层错误不透出")
	r := openRunner(t)
	boom := errors.New("disk io error on /dev/sda1 sector 99") // 底层细节
	tasks := []Task{
		okTask("ok-node", nil, "cfg", "out"),
		{Name: "bad-node", Config: []byte("cfg"), Run: func(ctx context.Context) ([]byte, error) {
			return nil, boom
		}},
		{Name: "downstream", Deps: []string{"bad-node"}, Config: []byte("cfg"),
			Run: func(ctx context.Context) ([]byte, error) { return []byte("x"), nil }},
	}
	rep, err := r.Run(context.Background(), tasks)
	if err != nil {
		t.Fatalf("运行不应返回错误（失败体现在报告中）: %v", err)
	}
	if rep.OK() {
		t.Fatalf("存在失败节点，报告不应为 OK")
	}

	entries, err := r.Audit().List(rep.RunID)
	if err != nil || len(entries) != 3 {
		t.Fatalf("应有 3 条审计记录: n=%d err=%v", len(entries), err)
	}
	byNode := map[string]string{}
	for _, e := range entries {
		byNode[e.Node] = e.Status
		switch e.Node {
		case "ok-node":
			if e.Status != "succeeded" || e.Duration < 0 {
				t.Fatalf("ok-node 审计异常: %+v", e)
			}
		case "bad-node":
			if e.Status != "failed" || e.Failure != "execution" || e.Attempts != 2 {
				t.Fatalf("bad-node 审计异常: %+v", e)
			}
			if contains(e.Message, "/dev/sda1") {
				t.Fatalf("底层错误细节透出了: %s", e.Message)
			}
		case "downstream":
			if e.Status != "skipped" {
				t.Fatalf("downstream 应为 skipped: %+v", e)
			}
		}
	}
	caseLog(t, "执行审计", "成功/失败(分类execution,重试2次)/跳过各一条，底层错误未透出", "通过")
}

// TestPipelineRerunIdempotent 业务：版本幂等 —— 同一输入整体重跑得到相同版本号。
func TestPipelineRerunIdempotent(t *testing.T) {
	t.Log("【业务覆盖】版本幂等：同一输入整体重跑，版本号一致且不产生重复版本")
	r := openRunner(t)
	tasks := []Task{
		okTask("extract", nil, "cfg-extract", "raw-data"),
		okTask("transform", []string{"extract"}, "cfg-transform", "clean-data"),
	}
	rep1, err := r.Run(context.Background(), tasks)
	if err != nil || !rep1.OK() {
		t.Fatalf("首次运行失败: %v", err)
	}
	rep2, err := r.Run(context.Background(), tasks)
	if err != nil || !rep2.OK() {
		t.Fatalf("重复运行失败: %v", err)
	}
	for _, n := range []string{"extract", "transform"} {
		if rep1.Nodes[n].VersionID != rep2.Nodes[n].VersionID {
			t.Fatalf("节点 %s 重跑版本号不一致: %s != %s",
				n, rep1.Nodes[n].VersionID, rep2.Nodes[n].VersionID)
		}
	}
	caseLog(t, "版本幂等", "同一输入整体重跑版本号完全一致", "通过")
}

// TestCycleRejectedAtPipelineLevel 业务：依赖调度 —— 成环在编排层被拒绝且分类正确。
func TestCycleRejectedAtPipelineLevel(t *testing.T) {
	t.Log("【业务覆盖】依赖调度：成环任务在编排层执行前被拒绝")
	r := openRunner(t)
	tasks := []Task{
		okTask("a", []string{"b"}, "c", "a"),
		okTask("b", []string{"a"}, "c", "b"),
	}
	_, err := r.Run(context.Background(), tasks)
	if err == nil || !errs.IsKind(err, errs.KindValidation) {
		t.Fatalf("成环应返回 validation 错误，实际: %v", err)
	}
	caseLog(t, "依赖调度", "a<->b 成环在执行前被编排层拒绝", "通过")
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
