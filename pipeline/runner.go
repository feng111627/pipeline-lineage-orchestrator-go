// Package pipeline 是本地数据流水线编排门面：
// 按依赖图调度节点执行，产出数据版本，记录血缘与执行审计。
// 对上层调用方只暴露分类后的安全错误，不透出底层细节。
package pipeline

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"time"

	"github.com/feng111627/pipeline-lineage-orchestrator-go/internal/audit"
	"github.com/feng111627/pipeline-lineage-orchestrator-go/internal/errs"
	"github.com/feng111627/pipeline-lineage-orchestrator-go/internal/lineage"
	"github.com/feng111627/pipeline-lineage-orchestrator-go/internal/scheduler"
	"github.com/feng111627/pipeline-lineage-orchestrator-go/internal/version"
)

// Task 描述一个数据处理节点。
type Task struct {
	Name   string
	Deps   []string
	Config []byte // 配置快照，其哈希进入血缘与输入指纹
	Run    func(ctx context.Context) ([]byte, error)
}

// NodeReport 是单个节点的运行报告。
type NodeReport struct {
	Status    scheduler.Status
	Attempts  int
	VersionID string // 成功时产出的版本号
	Duration  time.Duration
	Failure   audit.FailureKind
	Message   string // 对外安全信息
}

// Report 是一次流水线运行的完整报告。
type Report struct {
	RunID string
	Nodes map[string]NodeReport
}

// OK 表示全部节点成功。
func (r *Report) OK() bool {
	for _, n := range r.Nodes {
		if n.Status != scheduler.StatusSucceeded {
			return false
		}
	}
	return true
}

// Runner 是流水线编排器。
type Runner struct {
	versions    *version.Store
	lineage     *lineage.Store
	audit       *audit.Log
	retry       scheduler.RetryPolicy
	codeVersion string
}

// Open 在 dir 下打开（必要时创建）全部本地存储。
// codeVersion 标识当前处理逻辑代码版本，会记入血缘。
func Open(dir string, codeVersion string, retry scheduler.RetryPolicy) (*Runner, error) {
	vs, err := version.Open(dir + "/versions")
	if err != nil {
		return nil, err
	}
	ls, err := lineage.Open(dir + "/lineage")
	if err != nil {
		return nil, err
	}
	al, err := audit.Open(dir + "/audit")
	if err != nil {
		return nil, err
	}
	return &Runner{versions: vs, lineage: ls, audit: al, retry: retry, codeVersion: codeVersion}, nil
}

// Versions 暴露底层版本存储，供晋升/回滚/查询使用。
func (r *Runner) Versions() *version.Store { return r.versions }

// Lineage 暴露血缘存储，供血缘查询使用。
func (r *Runner) Lineage() *lineage.Store { return r.lineage }

// Audit 暴露审计日志，供执行记录查询使用。
func (r *Runner) Audit() *audit.Log { return r.audit }

// taskNode 把 Task 适配为 scheduler.Node。
type taskNode struct {
	t Task
}

func (n taskNode) Name() string                          { return n.t.Name }
func (n taskNode) Deps() []string                        { return n.t.Deps }
func (n taskNode) Run(ctx context.Context) ([]byte, error) { return n.t.Run(ctx) }

func hashOf(parts ...[]byte) string {
	h := sha256.New()
	for _, p := range parts {
		h.Write(p)
	}
	return hex.EncodeToString(h.Sum(nil))
}

// Run 校验依赖图并执行整条流水线。
// 依赖成环会在执行前被拒绝；失败节点的下游被明确跳过。
// 同一份输入（上游版本+配置）重复执行得到相同版本号，不产生重复版本。
func (r *Runner) Run(ctx context.Context, tasks []Task) (*Report, error) {
	nodes := make([]scheduler.Node, 0, len(tasks))
	byName := map[string]Task{}
	for _, t := range tasks {
		nodes = append(nodes, taskNode{t: t})
		byName[t.Name] = t
	}
	sched, err := scheduler.New(nodes, r.retry)
	if err != nil {
		return nil, err // 已是分类错误（如成环校验）
	}

	runID := fmt.Sprintf("run-%d", time.Now().UTC().UnixNano())
	results := sched.Run(ctx)

	rep := &Report{RunID: runID, Nodes: map[string]NodeReport{}}
	// 按拓扑顺序处理产物版本与血缘：上游版本号需先于下游确定。
	for _, name := range topoOrder(tasks) {
		res := results[name]
		nr := NodeReport{Status: res.Status, Attempts: res.Attempts, Duration: res.Duration}
		entry := audit.Entry{
			RunID: runID, Node: name, Status: string(res.Status),
			Attempts: res.Attempts, Duration: res.Duration, StartedAt: res.StartedAt,
		}
		switch res.Status {
		case scheduler.StatusSucceeded:
			vid, lerr := r.commitVersion(byName[name], res.Output, results, runID)
			if lerr != nil {
				return nil, lerr
			}
			nr.VersionID = vid
		case scheduler.StatusFailed:
			nr.Failure = audit.FailureExecution
			nr.Message = res.Err.Error() // errs.Error.Error() 只含安全信息
			entry.Failure = audit.FailureExecution
			entry.Message = nr.Message
		case scheduler.StatusSkipped:
			nr.Message = fmt.Sprintf("节点 %q 因上游失败被跳过", name)
			entry.Message = nr.Message
		}
		if aerr := r.audit.Append(entry); aerr != nil {
			return nil, aerr
		}
		rep.Nodes[name] = nr
	}
	return rep, nil
}

// commitVersion 为成功节点产出版本并记录血缘。输入指纹 = 配置哈希 + 全部上游版本号，
// 因此同一输入重复执行得到同一版本号（幂等，不产生重复版本）。
func (r *Runner) commitVersion(t Task, output []byte, results map[string]scheduler.Result, runID string) (string, error) {
	configHash := hashOf(t.Config)
	deps := append([]string{}, t.Deps...)
	sort.Strings(deps)
	var inputs []string
	fpParts := [][]byte{[]byte(configHash)}
	for _, d := range deps {
		up, err := r.versions.Current(d, "current")
		if err != nil {
			return "", errs.Wrap(errs.KindExecution, "pipeline.commitVersion",
				fmt.Sprintf("无法读取节点 %q 的上游 %q 的当前版本", t.Name, d), err)
		}
		inputs = append(inputs, up.Artifact+"@"+up.ID)
		fpParts = append(fpParts, []byte(up.ID))
	}
	inputHash := hashOf(fpParts...)

	v, _, err := r.versions.Commit(t.Name, inputHash, output)
	if err != nil {
		return "", err
	}
	if err := r.versions.Promote(t.Name, "current", v.ID, expectCurrentOf(r.versions, t.Name, v.ID)); err != nil {
		return "", err
	}
	if err := r.lineage.Add(lineage.Record{
		Artifact: t.Name, VersionID: v.ID, Inputs: inputs,
		ConfigHash: configHash, CodeVersion: r.codeVersion, RunID: runID,
		CreatedAt: time.Now().UTC(),
	}); err != nil {
		return "", err
	}
	return v.ID, nil
}

// expectCurrentOf 计算晋升通道的期望值：通道不存在则期望空，否则期望当前版本。
// 若本次产出版本与当前版本相同（幂等重跑），直接以当前版本为期望，保证可重入。
func expectCurrentOf(vs *version.Store, artifact, newID string) string {
	cur, err := vs.Current(artifact, "current")
	if err != nil {
		return ""
	}
	return cur.ID
}

// topoOrder 返回任务的拓扑执行顺序（仅用于确定版本提交顺序）。
func topoOrder(tasks []Task) []string {
	indeg := map[string]int{}
	adj := map[string][]string{}
	for _, t := range tasks {
		if _, ok := indeg[t.Name]; !ok {
			indeg[t.Name] = 0
		}
		for _, d := range t.Deps {
			adj[d] = append(adj[d], t.Name)
			indeg[t.Name]++
		}
	}
	var queue []string
	for name, d := range indeg {
		if d == 0 {
			queue = append(queue, name)
		}
	}
	sort.Strings(queue)
	var order []string
	for len(queue) > 0 {
		n := queue[0]
		queue = queue[1:]
		order = append(order, n)
		for _, m := range adj[n] {
			indeg[m]--
			if indeg[m] == 0 {
				queue = append(queue, m)
			}
		}
	}
	return order
}
