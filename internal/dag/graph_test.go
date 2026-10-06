package dag_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/feng111627/pipeline-lineage-orchestrator-go/internal/dag"
)

// 场景：成环检测 —— 报错信息中还原完整环路径。
func TestValidate_CyclePath(t *testing.T) {
	t.Log("业务场景：成环检测 —— 错误信息指出具体环路径")
	g := dag.New()
	g.AddEdge("a", "b")
	g.AddEdge("b", "c")
	g.AddEdge("c", "a")
	g.AddEdge("x", "y") // 无环部分不受影响
	err := g.Validate()
	if !errors.Is(err, dag.ErrCycle) {
		t.Fatalf("err = %v, want ErrCycle", err)
	}
	msg := err.Error()
	for _, n := range []string{"a", "b", "c"} {
		if !strings.Contains(msg, n) {
			t.Errorf("cycle message %q missing node %q", msg, n)
		}
	}
	t.Logf("结果：%v", err)
}

// 场景：分层拓扑 —— 同层无依赖可并行，下层依赖上层。
func TestLayers(t *testing.T) {
	t.Log("业务场景：分层拓扑 —— 无依赖同层，依赖关系跨层")
	g := dag.New()
	g.AddEdge("extract", "transform")
	g.AddEdge("extract", "audit")
	g.AddEdge("transform", "load")
	if err := g.Validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	layers := g.Layers()
	want := [][]string{{"extract"}, {"audit", "transform"}, {"load"}}
	if len(layers) != len(want) {
		t.Fatalf("layers = %v, want %v", layers, want)
	}
	for i := range want {
		if strings.Join(layers[i], ",") != strings.Join(want[i], ",") {
			t.Errorf("layer %d = %v, want %v", i, layers[i], want[i])
		}
	}
	t.Logf("结果：layers=%v", layers)
}
