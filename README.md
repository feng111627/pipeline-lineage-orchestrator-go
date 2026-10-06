# pipeline-lineage-orchestrator-go

纯本地的数据流水线编排器：按依赖图调度数据处理节点，产出数据自动版本化，
血缘与执行审计全部落盘在本地文件，不依赖任何外部服务。

## 模块结构

| 包 | 职责 |
|---|---|
| `internal/dag` | 依赖图建模、成环检测（报错含环路径）、分层拓扑排序 |
| `internal/orchestrator` | 按层并行执行、失败重试、上游失败时下游明确跳过 |
| `internal/version` | 本地文件版本存储：幂等产出、晋升、回滚、读取 |
| `internal/lineage` | JSONL 追加式血缘记录与执行审计 |
| `internal/pipeline` | 业务门面：把以上三者串成一条流水线，对上层只暴露语义化错误 |
| `cmd/pipeline` | 本地演示命令 |

## 依赖执行规则

- 节点通过 `Task.Deps` 声明依赖，构成有向图；执行前先做成环检测，
  **成环会在任何节点运行前被拒绝**，错误信息给出完整环路径，例如
  `dag: dependency cycle detected: a -> b -> c -> a`。
- 图被分成若干层：同层节点互不依赖、**并行执行**；下层节点必须等上层全部结束。
- 节点失败后按 `RetryPolicy{MaxAttempts, Backoff}` 重试；重试耗尽仍失败，
  其**所有下游节点（含间接下游）被标记为 `skipped` 且不会执行**，而不是带着坏数据继续跑。
- 引用未定义的节点、节点重名、空节点名都会在执行前报错。

## 版本与回滚语义

- 每次产出写入 `Produce(dataset, fingerprint, data)`，版本号从 1 单调递增，
  内容与元数据落在 `<root>/<dataset>/` 下（`v<N>.data` + `meta.json`，元数据原子提交）。
- **幂等**：指纹 = 输入版本指纹 + 配置哈希 + 代码版本 + 产出内容哈希。
  同一数据集上相同指纹重复 `Produce` 直接返回已有版本，**不会产生重复版本**，
  内容以首次写入为准，保证同样输入重复执行结果一致。
- 新产出的版本立即成为当前生效版本；`Promote` 为其打上"发布"标记。
- `Rollback(dataset, seq, expectCurrent)` 把当前版本切回历史版本，
  回滚后读取的内容与当时完全一致。
- **冲突即报错**：`Promote` / `Rollback` 都携带 `expectCurrent` 乐观校验，
  与当前状态不匹配时返回 `version.ErrConflict`，不做任何修改、绝不默默覆盖。

## 血缘与审计查询

```go
eng := pipeline.NewEngine(versionStore, lineageStore)
rep, _ := eng.Run(ctx, tasks)

// 血缘：产物 mart@1 由哪些输入、哪份配置、哪个代码版本跑出
rec, _ := eng.LineageOf("mart", 1)
// rec.Inputs = ["clean@1"]  rec.ConfigHash  rec.CodeVersion  rec.RunID

// 审计：某次运行每个节点的成败、类别、尝试次数与耗时
execs, _ := eng.ExecsOf(rep.RunID)
```

- 失败原因分类：`node_logic`（业务逻辑失败）、`retry_exhausted`（重试耗尽）、
  `skipped_upstream`（上游失败被跳过）、`internal`（编排器内部错误）。
- 审计中的 `Message` 是脱敏后的说明，底层错误原文不会透给上层；
  上层只能拿到 `pipeline.ErrCycle` / `pipeline.ErrRunFailed` / `version.Err*` 等语义化错误。

## 本地验证

```bash
# 跑全部单测（-v 日志中每个用例都打印业务场景与结果）
go test ./... -v

# 跑端到端演示：调度 -> 版本 -> 幂等重跑 -> 演进 -> 回滚 -> 审计
go run ./cmd/pipeline -dir /tmp/demo-pipeline
```

测试覆盖的业务场景：

| 场景 | 用例 |
|---|---|
| 依赖调度 | 同层并行、跨层有序（`orchestrator`）；分层拓扑（`dag`） |
| 失败重试 | 前 N 次失败后成功，按配置次数重试 |
| 下游短路 | 上游重试耗尽失败，直接/间接下游均 skipped 且未执行 |
| 成环拒绝 | 执行前拒绝并指出环路径（`dag` / `orchestrator` / `pipeline`） |
| 版本幂等 | 相同指纹重复产出不冒重复版本；整条流水线重跑幂等 |
| 回滚 | 回滚后读取与历史版本完全一致；冲突时明确报错不覆盖 |
| 血缘审计 | 血缘可回溯输入/配置/代码版本；审计分类记录且错误脱敏 |
