// 本地演示：跑一条 extract -> transform -> load 流水线，
// 展示版本产出、血缘查询、审计记录与回滚。全部状态在 ./demo-data 下。
package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/feng111627/pipeline-lineage-orchestrator-go/internal/scheduler"
	"github.com/feng111627/pipeline-lineage-orchestrator-go/pipeline"
)

func main() {
	dir := "demo-data"
	os.RemoveAll(dir)

	r, err := pipeline.Open(dir, "demo-code-v1", scheduler.RetryPolicy{MaxAttempts: 2})
	if err != nil {
		log.Fatalf("打开编排器失败: %v", err)
	}

	tasks := []pipeline.Task{
		{Name: "extract", Config: []byte("source=local-csv"),
			Run: func(ctx context.Context) ([]byte, error) { return []byte("id,amount\n1,10\n2,20"), nil }},
		{Name: "transform", Deps: []string{"extract"}, Config: []byte("agg=sum"),
			Run: func(ctx context.Context) ([]byte, error) { return []byte("total=30"), nil }},
		{Name: "load", Deps: []string{"transform"}, Config: []byte("target=local-file"),
			Run: func(ctx context.Context) ([]byte, error) { return []byte("loaded:total=30"), nil }},
	}

	rep, err := r.Run(context.Background(), tasks)
	if err != nil {
		log.Fatalf("运行失败: %v", err)
	}
	fmt.Printf("运行 %s 完成，OK=%v\n", rep.RunID, rep.OK())
	for name, n := range rep.Nodes {
		fmt.Printf("  节点 %-10s 状态=%s 版本=%s 耗时=%v\n", name, n.Status, n.VersionID, n.Duration)
	}

	// 血缘查询
	rec, err := r.Lineage().Of("load", rep.Nodes["load"].VersionID)
	if err != nil {
		log.Fatalf("血缘查询失败: %v", err)
	}
	fmt.Printf("血缘: %s\n", rec)

	// 审计查询
	entries, _ := r.Audit().List(rep.RunID)
	for _, e := range entries {
		fmt.Printf("审计: 节点=%s 状态=%s 尝试=%d 耗时=%v 失败分类=%s\n",
			e.Node, e.Status, e.Attempts, e.Duration, e.Failure)
	}

	// 回滚演示：再产出一个新版本晋升后回滚
	v2, _, err := r.Versions().Commit("load", "manual-input-2", []byte("loaded:total=31"))
	if err != nil {
		log.Fatalf("产出 v2 失败: %v", err)
	}
	cur, _ := r.Versions().Current("load", "current")
	if err := r.Versions().Promote("load", "current", v2.ID, cur.ID); err != nil {
		log.Fatalf("晋升 v2 失败: %v", err)
	}
	if err := r.Versions().Rollback("load", "current", cur.ID, v2.ID); err != nil {
		log.Fatalf("回滚失败: %v", err)
	}
	after, _ := r.Versions().Current("load", "current")
	fmt.Printf("回滚后 load 当前版本=%s 数据=%s（与回滚目标完全一致）\n", after.ID, after.Data)
}
