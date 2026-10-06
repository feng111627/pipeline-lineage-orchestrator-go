// Package pipeline 是业务门面：把依赖编排、数据版本与血缘审计串成
// 一条本地数据流水线。对上层只暴露语义化错误，不透出底层细节。
package pipeline

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/feng111627/pipeline-lineage-orchestrator-go/internal/dag"
	"github.com/feng111627/pipeline-lineage-orchestrator-go/internal/lineage"
	"github.com/feng111627/pipeline-lineage-orchestrator-go/internal/orchestrator"
	"github.com/feng111627/pipeline-lineage-orchestrator-go/internal/version"
)

// 对上层暴露的语义化错误。
var (
	ErrCycle       = dag.ErrCycle
	ErrRunFailed   = errors.New("pipeline: run finished with failed nodes")
	ErrLineageMiss = errors.New("pipeline: lineage record not found")
)

// Task 描述一个数据处理节点。
type Task struct {
	Name        string
	Deps        []string // 依赖的任务名
	Dataset     string   // 产出数据集名（为空则不落版本）
	Inputs      []string // 输入数据集名，参与指纹与血缘
	Config      any      // 任务配置，参与指纹
	CodeVersion string   // 代码版本标识，参与指纹
	Retry       orchestrator.RetryPolicy
	Run         func(ctx context.Context) ([]byte, error) // 业务逻辑，返回产物内容
}

// NodeReport 是单个任务的执行报告。
type NodeReport struct {
	Name     string
	Status   orchestrator.Status
	Attempts int
	Duration time.Duration
	Dataset  string
	Seq      int // 产出的数据版本号，未产出为 0
}

// Report 是一次流水线运行的完整报告。
type Report struct {
	RunID string
	Nodes []NodeReport
}

// Engine 是流水线引擎。
type Engine struct {
	vs *version.Store
	ls *lineage.Store
}

// NewEngine 以本地版本存储与血缘存储构建引擎。
func NewEngine(vs *version.Store, ls *lineage.Store) *Engine {
	return &Engine{vs: vs, ls: ls}
}

// Run 校验依赖关系并执行全部任务，记录版本、血缘与审计。
// 依赖成环在执行前被拒绝；有任务失败时返回 ErrRunFailed。
func (e *Engine) Run(ctx context.Context, tasks []Task) (*Report, error) {
	runID := newRunID()
	byName := map[string]Task{}
	for _, t := range tasks {
		byName[t.Name] = t
	}

	nodes := make([]orchestrator.Node, 0, len(tasks))
	produced := map[string]*NodeReport{} // name -> report（含产出版本）
	for _, t := range tasks {
		t := t
		rep := &NodeReport{Name: t.Name, Dataset: t.Dataset}
		produced[t.Name] = rep
		nodes = append(nodes, orchestrator.Node{
			Name:  t.Name,
			Deps:  t.Deps,
			Retry: t.Retry,
			Run: func(ctx context.Context) error {
				return e.runOne(ctx, runID, t, rep)
			},
		})
	}

	results, err := orchestrator.NewRunner().Run(ctx, nodes)
	if err != nil && !errors.Is(err, orchestrator.ErrFailed) {
		// 成环 / 图非法：执行前拒绝，直接上抛（dag 错误已含环路径）
		return nil, err
	}

	rep := &Report{RunID: runID}
	for _, t := range tasks {
		nr := produced[t.Name]
		res := results[t.Name]
		nr.Status = res.Status
		nr.Attempts = res.Attempts
		nr.Duration = res.Duration
		rep.Nodes = append(rep.Nodes, *nr)
		e.audit(runID, t, res)
	}
	if err != nil {
		return rep, ErrRunFailed
	}
	return rep, nil
}

// runOne 执行单个任务：跑业务逻辑、算指纹、落版本、记血缘。
func (e *Engine) runOne(ctx context.Context, runID string, t Task, rep *NodeReport) error {
	data, err := t.Run(ctx)
	if err != nil {
		return err // 由编排器记录为节点失败，审计时脱敏
	}
	if t.Dataset == "" {
		return nil // 纯计算节点，不落版本
	}
	// 指纹 = 输入版本指纹 + 配置 + 代码版本 + 产出内容哈希。
	// 确定性任务对同样输入产出同样内容，故重复执行命中同一指纹（幂等）；
	// 输入或内容变化则指纹变化，产生新版本。
	fp, inputs, err := e.fingerprint(t, data)
	if err != nil {
		return err
	}
	v, err := e.vs.Produce(t.Dataset, fp, data)
	if err != nil {
		return fmt.Errorf("produce version: %w", err)
	}
	rep.Seq = v.Seq
	return e.ls.AppendLineage(lineage.Record{
		Artifact:    t.Dataset,
		DatasetSeq:  v.Seq,
		Inputs:      inputs,
		ConfigHash:  configHash(t.Config),
		CodeVersion: t.CodeVersion,
		RunID:       runID,
	})
}

// fingerprint 计算任务指纹：输入数据集的当前版本指纹 + 配置 + 代码版本
// + 产出内容哈希。同时返回 "dataset@seq" 形式的输入血缘列表。
func (e *Engine) fingerprint(t Task, data []byte) (string, []string, error) {
	contentSum := sha256.Sum256(data)
	parts := []string{
		"code:" + t.CodeVersion,
		"config:" + configHash(t.Config),
		"content:" + hex.EncodeToString(contentSum[:]),
	}
	var inputs []string
	for _, in := range t.Inputs {
		v, err := e.vs.Current(in)
		if err != nil {
			return "", nil, fmt.Errorf("resolve input %q: %w", in, err)
		}
		inputs = append(inputs, fmt.Sprintf("%s@%d", in, v.Seq))
		parts = append(parts, fmt.Sprintf("in:%s:%s", in, v.Fingerprint))
	}
	sort.Strings(parts)
	sum := sha256.Sum256([]byte(strings.Join(parts, "|")))
	return hex.EncodeToString(sum[:]), inputs, nil
}

// audit 把节点执行结果写入审计日志，失败原因分类且信息脱敏。
func (e *Engine) audit(runID string, t Task, res orchestrator.Result) {
	rec := lineage.ExecRecord{
		RunID:    runID,
		Node:     t.Name,
		Attempts: res.Attempts,
		Duration: res.Duration,
	}
	switch res.Status {
	case orchestrator.StatusSuccess:
		rec.Status = "success"
	case orchestrator.StatusSkipped:
		rec.Status = "skipped"
		rec.Kind = lineage.FailureSkipped
		rec.Message = "skipped because an upstream dependency did not succeed"
	case orchestrator.StatusFailed:
		rec.Status = "failed"
		if res.Attempts > 1 {
			rec.Kind = lineage.FailureRetryOut
			rec.Message = fmt.Sprintf("node failed after %d attempts", res.Attempts)
		} else {
			rec.Kind = lineage.FailureNodeLogic
			rec.Message = "node logic returned an error"
		}
	}
	_ = e.ls.AppendExec(rec) // 审计写失败不阻断主流程
}

// LineageOf 查询某产物某版本的血缘：输入、配置指纹与代码版本。
func (e *Engine) LineageOf(artifact string, seq int) (lineage.Record, error) {
	r, err := e.ls.LineageOf(artifact, seq)
	if err != nil {
		return lineage.Record{}, ErrLineageMiss
	}
	return r, nil
}

// ExecsOf 查询某次运行的全部执行审计记录。
func (e *Engine) ExecsOf(runID string) ([]lineage.ExecRecord, error) {
	return e.ls.ExecsOf(runID)
}

func configHash(c any) string {
	if c == nil {
		return ""
	}
	b, err := json.Marshal(c)
	if err != nil {
		return ""
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func newRunID() string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return fmt.Sprintf("run-%d-%s", time.Now().Unix(), hex.EncodeToString(b[:]))
}
