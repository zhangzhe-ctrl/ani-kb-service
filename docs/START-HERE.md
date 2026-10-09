# START-HERE — kb-service 拆分文档导航

> 本目录是 kb-service 拆分（ADR 0001）的文档结构起点。monorepo 阶段（`feat/split-kb-from-core` 分支）以本目录为拆分事务的唯一文档入口；仓库拆分（阶段 5）时整目录随服务迁入独立仓库。

## 导航（按先读顺序）

| 路径 | 用途 | 先读顺序 |
|---|---|---|
| [adr/0001-split-kb-from-core.md](adr/0001-split-kb-from-core.md) | 拆分 ADR（第一条 ADR）：领域界定、四个裁定点（D1-D4）、现状耦合盘点、分阶段方案 | 1 |
| [execution/status.md](execution/status.md) | 执行状态唯一真实来源：基线快照 commit、冻结纪律、耦合差异清单、协作知会回执、当前阶段 | 2 |
| [execution/records/](execution/records/) | 批次过程记录与协作排期知会/回执存档 | 按需 |
| [specs/](specs/) | 行为规格（状态机、幂等、降级、ack 语义等），阶段 1 起逐批次落档 | 按需 |
| [runbook/](runbook/) | 运维手册（存储运维归属落地：Milvus 监控/备份/容量、etcd 配额与 compaction 教训、bucket 生命周期/配额、预签名时效） | 按需 |

## 规划侧镜像（monorepo，阶段 5 前保留）

- ADR 规划侧文件（规划期历史稿，已冻结，仅含状态头注）：`repo/services/tasks/modules/plan/knowledge_base/0001-split-kb-from-core.md`
- 拆分 issue 清单（#049-#070）：`repo/services/tasks/modules/issue/core/knowledge/issue-0*.md`

## 冲突裁决

- 架构裁定以 `adr/0001-split-kb-from-core.md` 为准：**本目录的 0001 是拆分执行期的唯一权威文本**；monorepo plan 模块中的同名文件是规划期历史稿（阶段 5 随 monorepo 收尾归档），此后不再单独修订——修订只发生在本目录，避免双副本漂移。ADR 状态变更（Proposed → Accepted）在阶段 5 收尾批次（#070）执行。
- 执行进度/状态以 `execution/status.md` 为准
- 冻结纪律：**拆分期间平移优先、不做功能性重构**（ADR 0001 §三-4）；违反该纪律的改动不得合入
