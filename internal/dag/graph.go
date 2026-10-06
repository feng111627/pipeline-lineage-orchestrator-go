// Package dag 提供有向无环图的建模、校验与分层拓扑排序能力。
package dag

import (
	"fmt"
	"sort"
	"strings"
)

// ErrCycle 表示依赖图中存在环，在执行前被拒绝。
var ErrCycle = fmt.Errorf("dag: dependency cycle detected")

// Graph 描述节点之间的依赖关系：边 from -> to 表示 to 依赖 from。
type Graph struct {
	nodes map[string]struct{}
	edges map[string][]string // from -> []to（下游）
}

// New 创建空图。
func New() *Graph {
	return &Graph{nodes: map[string]struct{}{}, edges: map[string][]string{}}
}

// AddNode 注册一个节点，重复注册为幂等操作。
func (g *Graph) AddNode(name string) {
	g.nodes[name] = struct{}{}
}

// AddEdge 添加依赖边 from -> to，表示 to 依赖 from。
// 两端节点若未注册会被自动注册。
func (g *Graph) AddEdge(from, to string) {
	g.AddNode(from)
	g.AddNode(to)
	for _, d := range g.edges[from] {
		if d == to {
			return // 幂等
		}
	}
	g.edges[from] = append(g.edges[from], to)
}

// Validate 校验图是否无环；若成环，返回携带环上节点路径的错误。
// 使用三色 DFS，发现回边时从栈中还原完整环路径。
func (g *Graph) Validate() error {
	const (
		white = 0 // 未访问
		gray  = 1 // 在栈上
		black = 2 // 已完成
	)
	color := map[string]int{}
	var stack []string

	names := g.sortedNodes()
	var visit func(n string) error
	visit = func(n string) error {
		color[n] = gray
		stack = append(stack, n)
		for _, next := range g.edges[n] {
			switch color[next] {
			case gray:
				// 找到环：从栈中截取 next 到当前节点的路径
				idx := 0
				for i, s := range stack {
					if s == next {
						idx = i
						break
					}
				}
				cycle := append(append([]string{}, stack[idx:]...), next)
				return fmt.Errorf("%w: %s", ErrCycle, strings.Join(cycle, " -> "))
			case white:
				if err := visit(next); err != nil {
					return err
				}
			}
		}
		stack = stack[:len(stack)-1]
		color[n] = black
		return nil
	}
	for _, n := range names {
		if color[n] == white {
			if err := visit(n); err != nil {
				return err
			}
		}
	}
	return nil
}

// Layers 返回分层拓扑序：同层节点互不依赖可并行，下层依赖上层。
// 调用前必须先通过 Validate；为确定性，同层节点按名字排序。
func (g *Graph) Layers() [][]string {
	indeg := map[string]int{}
	for n := range g.nodes {
		indeg[n] = 0
	}
	for from, tos := range g.edges {
		_ = from
		for _, to := range tos {
			indeg[to]++
		}
	}
	var layers [][]string
	var cur []string
	for n, d := range indeg {
		if d == 0 {
			cur = append(cur, n)
		}
	}
	remaining := len(indeg)
	for len(cur) > 0 {
		sort.Strings(cur)
		layers = append(layers, cur)
		remaining -= len(cur)
		var next []string
		for _, n := range cur {
			for _, to := range g.edges[n] {
				indeg[to]--
				if indeg[to] == 0 {
					next = append(next, to)
				}
			}
		}
		cur = next
	}
	if remaining > 0 {
		// 图有环，Layers 不应用于有环图；返回已计算部分
		return layers
	}
	return layers
}

func (g *Graph) sortedNodes() []string {
	names := make([]string, 0, len(g.nodes))
	for n := range g.nodes {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}
