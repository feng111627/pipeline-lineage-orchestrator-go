package pipeline_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/feng111627/pipeline-lineage-orchestrator-go/internal/lineage"
	"github.com/feng111627/pipeline-lineage-orchestrator-go/internal/orchestrator"
	"github.com/feng111627/pipeline-lineage-orchestrator-go/internal/pipeline"
	"github.com/feng111627/pipeline-lineage-orchestrator-go/internal/version"
)

func newEngine(t *testing.T) (*pipeline.Engine, *version.Store) {
	t.Helper()
	dir := t.TempDir()
	vs, err := version.Open(dir + "/versions")
	if err != nil {
		t.Fatalf("version store: %v", err)
	}
	ls, err := lineage.Open(dir + "/lineage")
	if err != nil {
		t.Fatalf("lineage store: %v", err)
	}
	return pipeline.NewEngine(vs, ls), vs
}

// 场景：血缘查询 —— 产物可回溯到输入、配置指纹与代码版本。
func TestRun_LineageQueryable(t *testing.T) {
	t.Log("业务场景：血缘查询 —— 产物可回溯输入、配置与代码版本")
	eng, _ := newEngine(t)
	tasks := []pipeline.Task{
		{
			Name: "extract", Dataset: "raw", CodeVersion: "git:aaa",
			Config: map[string]any{"source": "csv"},
			Run:    func(ctx context.Context) ([]byte, error) { return []byte("raw-data"), nil },
		},
		{
			Name: "transform", Deps: []string{"extract"}, Dataset: "clean",
			Inputs: []string{"raw"}, CodeVersion: "git:bbb",
			Config: map[string]any{"dedup": true},
			Run:    func(ctx context.Context) ([]byte, error) { return []byte("clean-data"), nil },
		},
	}
	rep, err := eng.Run(context.Background(), tasks)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	rec, err := eng.LineageOf("clean", 1)
	if err != nil {
		t.Fatalf("lineage of clean@1: %v", err)
	}
	if len(rec.Inputs) != 1 || rec.Inputs[0] != "raw@1" {
		t.Errorf("inputs = %v, want [raw@1]", rec.Inputs)
	}
	if rec.CodeVersion != "git:bbb" {
		t.Errorf("code version = %q, want git:bbb", rec.CodeVersion)
	}
	if rec.ConfigHash == "" {
		t.Error("config hash must not be empty")
	}
	if rec.RunID != rep.RunID {
		t.Errorf("lineage runID = %q, want %q", rec.RunID, rep.RunID)
	}
	t.Logf("结果：clean@1 血缘 inputs=%v code=%s configHash=%.8s runID=%s",
		rec.Inputs, rec.CodeVersion, rec.ConfigHash, rec.RunID)
}

// 场景：执行审计 —— 成功/失败/跳过分类记录，失败信息脱敏。
func TestRun_ExecAudit(t *testing.T) {
	t.Log("业务场景：执行审计 —— 区分成功/重试耗尽失败/上游跳过，错误脱敏")
	eng, _ := newEngine(t)
	secret := errors.New("db password leaked: s3cret")
	tasks := []pipeline.Task{
		{
			Name: "bad", Dataset: "x",
			Retry: orchestrator.RetryPolicy{MaxAttempts: 2},
			Run:   func(ctx context.Context) ([]byte, error) { return nil, secret },
		},
		{
			Name: "child", Deps: []string{"bad"}, Dataset: "y", Inputs: []string{"x"},
			Run: func(ctx context.Context) ([]byte, error) { return []byte("y"), nil },
		},
		{
			Name: "ok", Dataset: "z",
			Run: func(ctx context.Context) ([]byte, error) { return []byte("z"), nil },
		},
	}
	rep, err := eng.Run(context.Background(), tasks)
	if !errors.Is(err, pipeline.ErrRunFailed) {
		t.Fatalf("err = %v, want ErrRunFailed", err)
	}
	execs, err := eng.ExecsOf(rep.RunID)
	if err != nil {
		t.Fatalf("execs: %v", err)
	}
	byNode := map[string]lineage.ExecRecord{}
	for _, e := range execs {
		byNode[e.Node] = e
	}
	if byNode["ok"].Status != "success" {
		t.Errorf("ok status = %q, want success", byNode["ok"].Status)
	}
	bad := byNode["bad"]
	if bad.Status != "failed" || bad.Kind != lineage.FailureRetryOut || bad.Attempts != 2 {
		t.Errorf("bad = %+v, want failed/retry_exhausted/2 attempts", bad)
	}
	child := byNode["child"]
	if child.Status != "skipped" || child.Kind != lineage.FailureSkipped {
		t.Errorf("child = %+v, want skipped/skipped_upstream", child)
	}
	// 底层错误原文不得出现在审计信息中
	for _, e := range execs {
		if strings.Contains(e.Message, "s3cret") {
			t.Errorf("audit message leaks underlying error: %q", e.Message)
		}
	}
	if bad.Duration <= 0 {
		t.Error("duration must be recorded")
	}
	t.Logf("结果：ok=success，bad=failed/%s/%d次，child=skipped/%s，耗时已记录，错误已脱敏",
		bad.Kind, bad.Attempts, child.Kind)
}

// 场景：重跑幂等 —— 同样输入重复执行整条流水线，不产生重复版本。
func TestRun_IdempotentRerun(t *testing.T) {
	t.Log("业务场景：重跑幂等 —— 相同输入重复执行不产生重复版本")
	eng, vs := newEngine(t)
	var runs atomic.Int32
	tasks := []pipeline.Task{
		{
			Name: "extract", Dataset: "raw", CodeVersion: "git:aaa",
			Config: map[string]any{"source": "csv"},
			Run: func(ctx context.Context) ([]byte, error) {
				runs.Add(1)
				return []byte("raw-data"), nil
			},
		},
	}
	rep1, err := eng.Run(context.Background(), tasks)
	if err != nil {
		t.Fatalf("first run: %v", err)
	}
	rep2, err := eng.Run(context.Background(), tasks)
	if err != nil {
		t.Fatalf("second run: %v", err)
	}
	versions, _ := vs.List("raw")
	if len(versions) != 1 {
		t.Errorf("version count after rerun = %d, want 1", len(versions))
	}
	if rep1.Nodes[0].Seq != rep2.Nodes[0].Seq {
		t.Errorf("seq changed across reruns: %d vs %d", rep1.Nodes[0].Seq, rep2.Nodes[0].Seq)
	}
	// 两次执行都有审计记录
	execs1, _ := eng.ExecsOf(rep1.RunID)
	execs2, _ := eng.ExecsOf(rep2.RunID)
	if len(execs1) != 1 || len(execs2) != 1 {
		t.Errorf("exec records = %d/%d, want 1/1", len(execs1), len(execs2))
	}
	t.Logf("结果：两次运行均得到 raw@%d，版本总数=%d，审计各 %d 条",
		rep1.Nodes[0].Seq, len(versions), len(execs1))
}

// 场景：依赖成环 —— 引擎在执行前拒绝并说明环。
func TestRun_CycleRejected(t *testing.T) {
	t.Log("业务场景：依赖成环 —— 引擎执行前拒绝并指明环路径")
	eng, _ := newEngine(t)
	tasks := []pipeline.Task{
		{Name: "a", Deps: []string{"b"}},
		{Name: "b", Deps: []string{"a"}},
	}
	_, err := eng.Run(context.Background(), tasks)
	if !errors.Is(err, pipeline.ErrCycle) {
		t.Fatalf("err = %v, want ErrCycle", err)
	}
	t.Logf("结果：%v", err)
}

// 场景：输入变更产生新版本 —— 上游变化后下游指纹变化，产生新版本。
func TestRun_InputChangeNewVersion(t *testing.T) {
	t.Log("业务场景：版本演进 —— 上游内容变化使下游产生新版本")
	eng, vs := newEngine(t)
	var rawData atomic.Value
	rawData.Store([]byte("v1"))
	tasks := func() []pipeline.Task {
		return []pipeline.Task{
			{
				Name: "extract", Dataset: "raw", CodeVersion: "git:aaa",
				Run: func(ctx context.Context) ([]byte, error) {
					return rawData.Load().([]byte), nil
				},
			},
			{
				Name: "transform", Deps: []string{"extract"}, Dataset: "clean",
				Inputs: []string{"raw"}, CodeVersion: "git:bbb",
				Run: func(ctx context.Context) ([]byte, error) {
					cur, _ := vs.Current("raw")
					b, _ := vs.Get("raw", cur.Seq)
					return []byte(fmt.Sprintf("clean(%s)", b)), nil
				},
			},
		}
	}
	if _, err := eng.Run(context.Background(), tasks()); err != nil {
		t.Fatalf("run 1: %v", err)
	}
	rawData.Store([]byte("v2")) // 上游内容变化
	if _, err := eng.Run(context.Background(), tasks()); err != nil {
		t.Fatalf("run 2: %v", err)
	}
	rawV, _ := vs.List("raw")
	cleanV, _ := vs.List("clean")
	if len(rawV) != 2 || len(cleanV) != 2 {
		t.Errorf("raw/clean versions = %d/%d, want 2/2", len(rawV), len(cleanV))
	}
	rec, err := eng.LineageOf("clean", 2)
	if err != nil || len(rec.Inputs) != 1 || rec.Inputs[0] != "raw@2" {
		t.Errorf("clean@2 lineage = %+v, err=%v, want input raw@2", rec, err)
	}
	t.Logf("结果：raw %d 版、clean %d 版，clean@2 血缘指向 raw@2", len(rawV), len(cleanV))
}
