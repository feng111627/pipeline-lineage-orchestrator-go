package orchestrator_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/feng111627/pipeline-lineage-orchestrator-go/internal/dag"
	"github.com/feng111627/pipeline-lineage-orchestrator-go/internal/orchestrator"
)

// 场景：依赖调度 —— 无依赖节点并行执行，有依赖的严格按拓扑顺序。
func TestRun_DependencyScheduling(t *testing.T) {
	t.Log("业务场景：依赖调度 —— 同层并行、跨层有序")
	var mu sync.Mutex
	var order []string
	record := func(name string) func(ctx context.Context) error {
		return func(ctx context.Context) error {
			mu.Lock()
			order = append(order, name)
			mu.Unlock()
			return nil
		}
	}
	// 并行探测：a、b 同层，若并行执行则重叠计数最大值 > 1
	var inFlight, maxInFlight atomic.Int32
	parallel := func(name string) func(ctx context.Context) error {
		return func(ctx context.Context) error {
			cur := inFlight.Add(1)
			for {
				old := maxInFlight.Load()
				if cur <= old || maxInFlight.CompareAndSwap(old, cur) {
					break
				}
			}
			time.Sleep(20 * time.Millisecond)
			inFlight.Add(-1)
			return record(name)(ctx)
		}
	}

	nodes := []orchestrator.Node{
		{Name: "a", Run: parallel("a")},
		{Name: "b", Run: parallel("b")},
		{Name: "c", Deps: []string{"a", "b"}, Run: record("c")},
	}
	results, err := orchestrator.NewRunner().Run(context.Background(), nodes)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	for name, want := range map[string]orchestrator.Status{"a": orchestrator.StatusSuccess, "b": orchestrator.StatusSuccess, "c": orchestrator.StatusSuccess} {
		if results[name].Status != want {
			t.Errorf("node %s status = %s, want %s", name, results[name].Status, want)
		}
	}
	// c 必须最后执行
	if order[len(order)-1] != "c" {
		t.Errorf("c must run after a and b, order = %v", order)
	}
	if maxInFlight.Load() < 2 {
		t.Errorf("a and b should run in parallel, maxInFlight = %d", maxInFlight.Load())
	}
	t.Logf("结果：执行顺序=%v，最大并发=%d，全部成功", order, maxInFlight.Load())
}

// 场景：失败重试 —— 前 N 次失败后成功，按配置次数重试。
func TestRun_RetryThenSuccess(t *testing.T) {
	t.Log("业务场景：失败重试 —— 节点前 2 次失败，第 3 次成功")
	var calls atomic.Int32
	nodes := []orchestrator.Node{{
		Name:  "flaky",
		Retry: orchestrator.RetryPolicy{MaxAttempts: 3},
		Run: func(ctx context.Context) error {
			if calls.Add(1) < 3 {
				return errors.New("temporary failure")
			}
			return nil
		},
	}}
	results, err := orchestrator.NewRunner().Run(context.Background(), nodes)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	res := results["flaky"]
	if res.Status != orchestrator.StatusSuccess {
		t.Errorf("status = %s, want success", res.Status)
	}
	if res.Attempts != 3 {
		t.Errorf("attempts = %d, want 3", res.Attempts)
	}
	t.Logf("结果：第 %d 次尝试成功，状态=%s", res.Attempts, res.Status)
}

// 场景：下游短路 —— 重试耗尽仍失败，下游被明确跳过而不是继续执行。
func TestRun_FailureSkipsDownstream(t *testing.T) {
	t.Log("业务场景：失败重试+下游短路 —— 上游重试耗尽失败，下游明确跳过")
	var calls, downstreamRan atomic.Int32
	nodes := []orchestrator.Node{
		{
			Name:  "bad",
			Retry: orchestrator.RetryPolicy{MaxAttempts: 2},
			Run: func(ctx context.Context) error {
				calls.Add(1)
				return errors.New("always fails")
			},
		},
		{
			Name: "downstream",
			Deps: []string{"bad"},
			Run: func(ctx context.Context) error {
				downstreamRan.Add(1)
				return nil
			},
		},
		{
			Name: "grandchild",
			Deps: []string{"downstream"},
			Run: func(ctx context.Context) error {
				downstreamRan.Add(1)
				return nil
			},
		},
	}
	results, err := orchestrator.NewRunner().Run(context.Background(), nodes)
	if !errors.Is(err, orchestrator.ErrFailed) {
		t.Fatalf("err = %v, want ErrFailed", err)
	}
	if got := calls.Load(); got != 2 {
		t.Errorf("bad node attempts = %d, want 2", got)
	}
	if results["bad"].Status != orchestrator.StatusFailed {
		t.Errorf("bad status = %s, want failed", results["bad"].Status)
	}
	for _, name := range []string{"downstream", "grandchild"} {
		if results[name].Status != orchestrator.StatusSkipped {
			t.Errorf("%s status = %s, want skipped", name, results[name].Status)
		}
	}
	if downstreamRan.Load() != 0 {
		t.Errorf("downstream must not run, ran %d times", downstreamRan.Load())
	}
	t.Logf("结果：bad 重试 %d 次后失败，downstream/grandchild 均 skipped 且未执行", calls.Load())
}

// 场景：依赖成环 —— 执行前被拒绝，错误中指明环路径。
func TestRun_CycleRejected(t *testing.T) {
	t.Log("业务场景：依赖成环 —— 执行前拒绝并指出环节点")
	nodes := []orchestrator.Node{
		{Name: "a", Deps: []string{"c"}},
		{Name: "b", Deps: []string{"a"}},
		{Name: "c", Deps: []string{"b"}},
		{Name: "ok"},
	}
	_, err := orchestrator.NewRunner().Run(context.Background(), nodes)
	if !errors.Is(err, dag.ErrCycle) {
		t.Fatalf("err = %v, want dag.ErrCycle", err)
	}
	t.Logf("结果：执行前拒绝，环信息：%v", err)
}

// 场景：引用未定义节点 —— 直接报错。
func TestRun_UndefinedDependency(t *testing.T) {
	t.Log("业务场景：依赖校验 —— 引用不存在的节点被拒绝")
	nodes := []orchestrator.Node{{Name: "a", Deps: []string{"ghost"}}}
	_, err := orchestrator.NewRunner().Run(context.Background(), nodes)
	if err == nil {
		t.Fatal("want error for undefined dependency")
	}
	t.Logf("结果：%v", err)
}
