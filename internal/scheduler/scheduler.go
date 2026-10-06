// Package scheduler 按依赖图调度节点执行：同层并行、失败重试、重试耗尽后下游短路跳过。
package scheduler

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/feng111627/pipeline-lineage-orchestrator-go/internal/dag"
	"github.com/feng111627/pipeline-lineage-orchestrator-go/internal/errs"
)

// Node 是一个可执行的数据处理节点。
type Node interface {
	// Name 返回节点名（图内唯一）。
	Name() string
	// Deps 返回直接依赖的节点名。
	Deps() []string
	// Run 执行节点逻辑，返回产物数据。
	Run(ctx context.Context) ([]byte, error)
}

// RetryPolicy 描述失败重试策略。
type RetryPolicy struct {
	MaxAttempts int           // 总尝试次数（含首次），<=1 表示不重试
	Backoff     time.Duration // 每次重试前的等待
}

// Status 是节点的最终执行状态。
type Status string

const (
	StatusSucceeded Status = "succeeded"
	StatusFailed    Status = "failed"
	StatusSkipped   Status = "skipped" // 因上游失败被短路
)

// Result 记录单个节点的执行结果。
type Result struct {
	Node      string
	Status    Status
	Attempts  int
	Output    []byte
	Err       error // 分类后的错误，不含底层细节
	Duration  time.Duration
	StartedAt time.Time
}

// Scheduler 按依赖图调度执行。
type Scheduler struct {
	graph *dag.Graph
	nodes map[string]Node
	retry RetryPolicy
	now   func() time.Time
	sleep func(context.Context, time.Duration) // 可注入，便于测试
}

// New 用给定节点与重试策略构建调度器；依赖成环会在此时被拒绝。
func New(nodes []Node, retry RetryPolicy) (*Scheduler, error) {
	g := dag.New()
	byName := map[string]Node{}
	for _, n := range nodes {
		if _, dup := byName[n.Name()]; dup {
			return nil, errs.Wrap(errs.KindValidation, "scheduler.New",
				fmt.Sprintf("节点名 %q 重复注册", n.Name()), nil)
		}
		byName[n.Name()] = n
		g.AddNode(n.Name())
	}
	for _, n := range nodes {
		for _, dep := range n.Deps() {
			if err := g.AddEdge(dep, n.Name()); err != nil {
				return nil, err
			}
		}
	}
	if err := g.Validate(); err != nil {
		return nil, err
	}
	if retry.MaxAttempts < 1 {
		retry.MaxAttempts = 1
	}
	return &Scheduler{
		graph: g,
		nodes: byName,
		retry: retry,
		now:   time.Now,
		sleep: func(ctx context.Context, d time.Duration) {
			select {
			case <-ctx.Done():
			case <-time.After(d):
			}
		},
	}, nil
}

// Run 执行全部节点，返回每个节点的结果（含被跳过的）。
func (s *Scheduler) Run(ctx context.Context) map[string]Result {
	layers, err := s.graph.Layers()
	if err != nil {
		// New 已校验，此处理论不可达；兜底返回空。
		return map[string]Result{}
	}
	results := map[string]Result{}
	var mu sync.Mutex

	for _, layer := range layers {
		var wg sync.WaitGroup
		for _, name := range layer {
			// 上游失败或被跳过 => 本节点短路跳过
			if s.blockedByFailure(name, results) {
				results[name] = Result{Node: name, Status: StatusSkipped}
				continue
			}
			wg.Add(1)
			go func(name string) {
				defer wg.Done()
				r := s.runWithRetry(ctx, s.nodes[name])
				mu.Lock()
				results[name] = r
				mu.Unlock()
			}(name)
		}
		wg.Wait()
	}
	return results
}

// blockedByFailure 判断节点的任一上游（含间接）是否失败或被跳过。
func (s *Scheduler) blockedByFailure(name string, results map[string]Result) bool {
	// 直接依赖即可判断：依赖是逐层执行的，间接上游的状态已传导到直接上游。
	for _, dep := range s.nodes[name].Deps() {
		if r, ok := results[dep]; ok && r.Status != StatusSucceeded {
			return true
		}
	}
	return false
}

func (s *Scheduler) runWithRetry(ctx context.Context, n Node) Result {
	start := s.now()
	var lastErr error
	attempts := 0
	for attempts = 1; attempts <= s.retry.MaxAttempts; attempts++ {
		out, err := n.Run(ctx)
		if err == nil {
			return Result{
				Node: n.Name(), Status: StatusSucceeded, Attempts: attempts,
				Output: out, Duration: s.now().Sub(start), StartedAt: start,
			}
		}
		lastErr = err
		if attempts < s.retry.MaxAttempts {
			s.sleep(ctx, s.retry.Backoff)
		}
	}
	return Result{
		Node: n.Name(), Status: StatusFailed, Attempts: s.retry.MaxAttempts,
		Err: errs.Wrap(errs.KindExecution, "scheduler.Run",
			fmt.Sprintf("节点 %q 重试 %d 次后仍失败", n.Name(), s.retry.MaxAttempts), lastErr),
		Duration:  s.now().Sub(start),
		StartedAt: start,
	}
}
