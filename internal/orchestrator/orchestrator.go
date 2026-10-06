// Package orchestrator 按依赖图调度节点执行：并行、重试、失败下游短路。
package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/feng111627/pipeline-lineage-orchestrator-go/internal/dag"
)

// Status 是节点的最终执行状态。
type Status string

const (
	StatusSuccess Status = "success"
	StatusFailed  Status = "failed"  // 重试耗尽后仍失败
	StatusSkipped Status = "skipped" // 上游失败被短路跳过
)

// ErrFailed 表示流水线中存在执行失败的节点。
var ErrFailed = errors.New("orchestrator: pipeline has failed nodes")

// RetryPolicy 描述失败重试策略。
type RetryPolicy struct {
	MaxAttempts int           // 总尝试次数，含首次；<=1 表示不重试
	Backoff     time.Duration // 每次重试前的等待间隔
}

// Node 是一个可执行节点。
type Node struct {
	Name  string
	Deps  []string // 依赖的上游节点名
	Retry RetryPolicy
	Run   func(ctx context.Context) error
}

// Result 记录单个节点的执行结果。
type Result struct {
	Name     string
	Status   Status
	Attempts int
	Duration time.Duration
	Err      error // 仅内部使用，向上暴露前需脱敏
}

// Runner 按依赖图执行一组节点。
type Runner struct{}

// NewRunner 创建执行器。
func NewRunner() *Runner { return &Runner{} }

// Run 校验依赖图并按层并行执行所有节点，返回每个节点的结果。
// 依赖成环时返回携带环节点的错误；有节点失败时返回 ErrFailed。
func (r *Runner) Run(ctx context.Context, nodes []Node) (map[string]Result, error) {
	g, err := buildGraph(nodes)
	if err != nil {
		return nil, err
	}
	if err := g.Validate(); err != nil {
		return nil, err // 成环：执行前拒绝，错误中含环路径
	}

	byName := map[string]Node{}
	for _, n := range nodes {
		byName[n.Name] = n
	}

	results := map[string]Result{}
	var mu sync.Mutex

	for _, layer := range g.Layers() {
		var wg sync.WaitGroup
		for _, name := range layer {
			n := byName[name]
			// 上游有失败/跳过：本节点明确跳过，不执行
			if upstreamBlocked(n, results) {
				mu.Lock()
				results[n.Name] = Result{Name: n.Name, Status: StatusSkipped}
				mu.Unlock()
				continue
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				res := r.runWithRetry(ctx, n)
				mu.Lock()
				results[n.Name] = res
				mu.Unlock()
			}()
		}
		wg.Wait()
	}

	for _, res := range results {
		if res.Status == StatusFailed {
			return results, ErrFailed
		}
	}
	return results, nil
}

// upstreamBlocked 判断节点是否因上游失败/跳过而应被短路。
func upstreamBlocked(n Node, results map[string]Result) bool {
	for _, dep := range n.Deps {
		if res, ok := results[dep]; ok && res.Status != StatusSuccess {
			return true
		}
	}
	return false
}

// runWithRetry 按重试策略执行单个节点。
func (r *Runner) runWithRetry(ctx context.Context, n Node) Result {
	maxAttempts := n.Retry.MaxAttempts
	if maxAttempts < 1 {
		maxAttempts = 1
	}
	start := time.Now()
	var lastErr error
	attempts := 0
	for attempts < maxAttempts {
		attempts++
		if err := ctx.Err(); err != nil {
			lastErr = err
			break
		}
		if n.Run == nil {
			lastErr = nil
			break // 无执行体视为成功（纯占位节点）
		}
		if err := n.Run(ctx); err != nil {
			lastErr = err
			if attempts < maxAttempts && n.Retry.Backoff > 0 {
				select {
				case <-ctx.Done():
					lastErr = ctx.Err()
					return Result{Name: n.Name, Status: StatusFailed, Attempts: attempts, Duration: time.Since(start), Err: lastErr}
				case <-time.After(n.Retry.Backoff):
				}
			}
			continue
		}
		lastErr = nil
		break
	}
	status := StatusSuccess
	if lastErr != nil {
		status = StatusFailed
	}
	return Result{
		Name:     n.Name,
		Status:   status,
		Attempts: attempts,
		Duration: time.Since(start),
		Err:      lastErr,
	}
}

// buildGraph 由节点列表构建依赖图；引用未定义节点时报错。
func buildGraph(nodes []Node) (*dag.Graph, error) {
	g := dag.New()
	seen := map[string]bool{}
	for _, n := range nodes {
		if n.Name == "" {
			return nil, fmt.Errorf("orchestrator: node with empty name")
		}
		if seen[n.Name] {
			return nil, fmt.Errorf("orchestrator: duplicate node %q", n.Name)
		}
		seen[n.Name] = true
		g.AddNode(n.Name)
	}
	for _, n := range nodes {
		for _, dep := range n.Deps {
			if !seen[dep] {
				return nil, fmt.Errorf("orchestrator: node %q depends on undefined node %q", n.Name, dep)
			}
			g.AddEdge(dep, n.Name)
		}
	}
	return g, nil
}
