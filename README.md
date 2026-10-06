# pipeline-lineage-orchestrator-go

纯本地的数据流水线编排器：按依赖图调度数据处理节点，产出可版本化的数据，
并记录完整血缘与执行审计。不依赖任何外部服务，全部状态落在本地目录的
JSON / JSONL 文件上。

## 结构

| 包 | 职责 |
| --- | --- |
| `internal/dag` | 依赖图：成环检测、分层拓扑排序、下游查询 |
| `internal/scheduler` | 调度执行：同层并行、失败重试、下游短路 |
| `internal/version` | 数据版本：产出（幂等）、晋升、回滚，冲突显式报错 |
| `internal/lineage` | 血缘：产物版本 ← 输入/配置哈希/代码版本/运行ID |
| `internal/audit` | 审计：每次执行的状态、耗时、失败分类 |
| `internal/errs` | 错误分类：validation / conflict / not_found / execution |
| `pipeline` | 编排门面，串起以上全部能力 |

## 依赖执行规则

- 节点声明 `Deps`（直接依赖），编排器据此建图。
- **成环即拒**：`pipeline.Run` 在真正执行任何节点前做全图成环检测，
  发现环立即返回 `validation` 类错误，并指明环上的节点序列
  （如 `x -> y -> z -> x`）。
- **同层并行**：拓扑分层后，同一层内互不依赖的节点并发执行；层与层之间严格有序。
- **失败重试**：节点失败后按 `RetryPolicy{MaxAttempts, Backoff}` 重试，
  `MaxAttempts` 为总尝试次数（含首次）。
- **下游短路**：重试耗尽仍失败的节点标记为 `failed`，其全部直接与间接下游
  标记为 `skipped` 且**不会被执行**；与失败节点无依赖关系的节点不受影响。

## 版本与回滚语义

- **版本号派生**：`版本号 = hash(输入指纹, 内容哈希)`。其中输入指纹 =
  `hash(配置哈希, 全部上游版本号)`，因此**同一份输入重复执行得到相同版本号，
  不会产生重复版本**（`Commit` 返回 `created=false`）。
- **冲突不覆盖**：同一输入指纹若对应到不同内容，`Commit` 返回 `conflict`
  错误，原版本数据保持不变。
- **晋升（Promote）**：把版本挂到通道（如 `current`）。采用乐观并发：
  必须给出 `expectCurrent`（期望的当前版本），与实际不符即返回 `conflict`，
  绝不默默覆盖。重复晋升同一版本是幂等空操作。
- **回滚（Rollback）**：把通道指回历史版本，同样要求 `expectCurrent`，
  不符即冲突且通道不变。版本数据不可变，因此**回滚后查询结果与历史版本
  当时完全一致**。

## 血缘与审计查询

```go
r, _ := pipeline.Open(dir, "my-code-v1", scheduler.RetryPolicy{MaxAttempts: 3})
rep, _ := r.Run(ctx, tasks)

// 血缘：某产物某版本由哪些输入、哪份配置、哪个代码版本、哪次运行产出
rec, _ := r.Lineage().Of("transform", rep.Nodes["transform"].VersionID)
// rec.Inputs      形如 ["extract@v-xxxx"]
// rec.ConfigHash  配置快照哈希
// rec.CodeVersion 代码版本
// rec.RunID       运行 ID

// 审计：某次运行每个节点的状态、尝试次数、耗时、失败分类
entries, _ := r.Audit().List(rep.RunID)
```

失败分类（`audit.FailureKind`）：`validation` / `execution` / `conflict` /
`not_found`。底层错误（如 IO、驱动报错）只保留在错误链内部
（`errs.Error.Cause()`），对上层暴露的 `Error()` 与审计 `Message`
只含业务化安全信息，**不会原样透出底层细节**。

## 本地验证

```bash
# 运行全部单测（-v 日志中可看到每类业务的用例与结果）
go test ./... -v

# 运行本地演示：调度 + 版本 + 血缘 + 审计 + 回滚
go run ./cmd/pipeline
```

单测覆盖四类核心业务，日志中以 `【业务覆盖】` / `[业务] ... | 用例: ... | 结果: ...`
标注：

- **依赖调度**：菱形依赖按层成功、同层并行（耗时 < 串行）、成环执行前拒绝并指明环节点
  （`internal/scheduler/scheduler_test.go`、`pipeline/runner_test.go`）
- **失败重试与下游短路**：重试内成功、重试耗尽后失败、直接+间接下游跳过、
  独立节点不受影响、底层错误不透出（`internal/scheduler/scheduler_test.go`）
- **版本幂等**：同一输入重复产出同一版本号、同指纹不同内容报冲突、
  整体重跑版本号一致（`internal/version/store_test.go`、`pipeline/runner_test.go`）
- **回滚**：回滚后查询与历史版本完全一致、期望不符冲突且通道不变、
  目标版本不存在报 `not_found`（`internal/version/store_test.go`）

演示程序会在 `./demo-data/` 下生成全部本地状态（版本、血缘、审计），可直接查看：

```bash
find demo-data -type f
```
