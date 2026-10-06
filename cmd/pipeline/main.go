// 本地演示：跑一条 extract -> transform -> load 流水线，
// 打印执行报告、血缘与回滚效果。全部数据落在 ./demo-data（可用 -dir 修改）。
package main

import (
	"context"
	"flag"
	"fmt"
	"log"

	"github.com/feng111627/pipeline-lineage-orchestrator-go/internal/lineage"
	"github.com/feng111627/pipeline-lineage-orchestrator-go/internal/pipeline"
	"github.com/feng111627/pipeline-lineage-orchestrator-go/internal/version"
)

func main() {
	dir := flag.String("dir", "demo-data", "本地数据目录")
	flag.Parse()

	vs, err := version.Open(*dir + "/versions")
	if err != nil {
		log.Fatalf("open version store: %v", err)
	}
	ls, err := lineage.Open(*dir + "/lineage")
	if err != nil {
		log.Fatalf("open lineage store: %v", err)
	}
	eng := pipeline.NewEngine(vs, ls)

	tasks := []pipeline.Task{
		{
			Name: "extract", Dataset: "raw", CodeVersion: "git:demo-1",
			Config: map[string]any{"source": "local.csv"},
			Run:    func(ctx context.Context) ([]byte, error) { return []byte("id,name\n1,alice\n"), nil },
		},
		{
			Name: "transform", Deps: []string{"extract"}, Dataset: "clean",
			Inputs: []string{"raw"}, CodeVersion: "git:demo-1",
			Config: map[string]any{"upper": true},
			Run:    func(ctx context.Context) ([]byte, error) { return []byte("ID,NAME\n1,ALICE\n"), nil },
		},
		{
			Name: "load", Deps: []string{"transform"}, Dataset: "mart",
			Inputs: []string{"clean"}, CodeVersion: "git:demo-1",
			Config: map[string]any{"target": "mart.daily"},
			Run:    func(ctx context.Context) ([]byte, error) { return []byte("mart rows: 1\n"), nil },
		},
	}

	rep, err := eng.Run(context.Background(), tasks)
	fmt.Printf("== 运行 %s ==\n", rep.RunID)
	for _, n := range rep.Nodes {
		fmt.Printf("  %-10s %-8s 尝试 %d 次  耗时 %s  产出 %s@%d\n",
			n.Name, n.Status, n.Attempts, n.Duration, n.Dataset, n.Seq)
	}
	if err != nil {
		fmt.Printf("运行结果: %v\n", err)
	}

	fmt.Println("== 血缘：mart@1 ==")
	if rec, err := eng.LineageOf("mart", 1); err == nil {
		fmt.Printf("  输入=%v 配置指纹=%.12s 代码版本=%s\n", rec.Inputs, rec.ConfigHash, rec.CodeVersion)
	}

	fmt.Println("== 重复执行（幂等验证）==")
	rep2, _ := eng.Run(context.Background(), tasks)
	versions, _ := vs.List("mart")
	fmt.Printf("  第二次运行 mart@%d，mart 版本总数=%d（不重复）\n", rep2.Nodes[2].Seq, len(versions))

	fmt.Println("== 版本演进与回滚 ==")
	tasks[2].Run = func(ctx context.Context) ([]byte, error) { return []byte("mart rows: 2\n"), nil }
	if _, err := eng.Run(context.Background(), tasks); err != nil {
		fmt.Printf("  第三次运行: %v\n", err)
	}
	cur, _ := vs.Current("mart")
	fmt.Printf("  内容变化后当前版本 mart@%d\n", cur.Seq)
	if _, err := vs.Rollback("mart", 1, cur.Seq); err != nil {
		fmt.Printf("  回滚失败: %v\n", err)
	}
	b, _ := vs.Get("mart", 1)
	cur, _ = vs.Current("mart")
	fmt.Printf("  回滚到 mart@%d，内容=%q（与当时一致）\n", cur.Seq, string(b))

	fmt.Println("== 执行审计 ==")
	execs, _ := eng.ExecsOf(rep.RunID)
	for _, e := range execs {
		fmt.Printf("  %-10s %-8s 尝试 %d 次  耗时 %s\n", e.Node, e.Status, e.Attempts, e.Duration)
	}
}
