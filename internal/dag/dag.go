// Package dag 提供依赖图的构建、成环检测与分层拓扑排序。
package dag

import (
	"fmt"
	"sort"
	"strings"

	"github.com/feng111627/pipeline-lineage-orchestrator-go/internal/errs"
)

// Graph 是一张以节点名为键的有向图（边 from->to 表示 from 先执行）。
type Graph struct {
	nodes map[string]struct{}
	edges map[string][]string // from -> to
}

// New 创建空图。
func New() *Graph {
	return &Graph{nodes: map[string]struct{}{}, edges: map[string][]string{}}
}

// AddNode 注册节点。
func (g *Graph) AddNode(name string) {
	g.nodes[name] = struct{}{}
}

// AddEdge 添加依赖边：from 必须先于 to 执行。引用未注册节点时报校验错误。
func (g *Graph) AddEdge(from, to string) error {
	if _, ok := g.nodes[from]; !ok {
		return errs.Wrap(errs.KindValidation, "dag.AddEdge",
			fmt.Sprintf("依赖边引用了未注册的节点 %q", from), nil)
	}
	if _, ok := g.nodes[to]; !ok {
		return errs.Wrap(errs.KindValidation, "dag.AddEdge",
			fmt.Sprintf("依赖边引用了未注册的节点 %q", to), nil)
	}
	g.edges[from] = append(g.edges[from], to)
	return nil
}

// findCycle 用三色 DFS 找一条环，返回环上节点序列；无环返回 nil。
func (g *Graph) findCycle() []string {
	const (
		white = 0 // 未访问
		gray  = 1 // 在栈上
		black = 2 // 已完成
	)
	color := map[string]int{}
	var stack []string
	var cycle []string

	var visit func(n string) bool
	visit = func(n string) bool {
		color[n] = gray
		stack = append(stack, n)
		for _, m := range g.edges[n] {
			if color[m] == gray {
				// 找到环：从栈中 m 的位置到栈顶
				idx := 0
				for i, s := range stack {
					if s == m {
						idx = i
						break
					}
				}
				cycle = append(append([]string{}, stack[idx:]...), m)
				return true
			}
			if color[m] == white && visit(m) {
				return true
			}
		}
		stack = stack[:len(stack)-1]
		color[n] = black
		return false
	}

	names := g.sortedNodes()
	for _, n := range names {
		if color[n] == white {
			if visit(n) {
				return cycle
			}
		}
	}
	return nil
}

// Validate 校验图无环；若成环，返回指明环上节点的校验错误。
func (g *Graph) Validate() error {
	if c := g.findCycle(); c != nil {
		return errs.Wrap(errs.KindValidation, "dag.Validate",
			fmt.Sprintf("依赖关系成环，拒绝执行：%s", strings.Join(c, " -> ")), nil)
	}
	return nil
}

// Layers 返回拓扑分层：同层节点互不依赖，可并行执行；成环时报错。
func (g *Graph) Layers() ([][]string, error) {
	if err := g.Validate(); err != nil {
		return nil, err
	}
	indeg := map[string]int{}
	for n := range g.nodes {
		indeg[n] = 0
	}
	for _, tos := range g.edges {
		for _, to := range tos {
			indeg[to]++
		}
	}
	var layers [][]string
	remaining := len(g.nodes)
	for remaining > 0 {
		var layer []string
		for n, d := range indeg {
			if d == 0 {
				layer = append(layer, n)
			}
		}
		sort.Strings(layer)
		layers = append(layers, layer)
		for _, n := range layer {
			delete(indeg, n)
			remaining--
			for _, to := range g.edges[n] {
				indeg[to]--
			}
		}
	}
	return layers, nil
}

// Downstream 返回某节点的全部下游节点（含间接下游）。
func (g *Graph) Downstream(name string) map[string]bool {
	out := map[string]bool{}
	queue := append([]string{}, g.edges[name]...)
	for len(queue) > 0 {
		n := queue[0]
		queue = queue[1:]
		if out[n] {
			continue
		}
		out[n] = true
		queue = append(queue, g.edges[n]...)
	}
	return out
}

func (g *Graph) sortedNodes() []string {
	names := make([]string, 0, len(g.nodes))
	for n := range g.nodes {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}
