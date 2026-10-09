# 运行合同与可观测性规格（KB-SPLIT-P3C / issue #063）

> 本文件落档 kb-service 拆分事务中**运行合同保持**与**领域指标/日志关联**
> 的行为规格。执行期权威裁定见 [ADR 0001](../adr/0001-split-kb-from-core.md)
> §五-阶段3（运行合同：`/readyz` 门控语义、领域指标、日志关联、保持消费者
> 先订阅后投递/双 loop/JetStream ack 语义/优雅停机逆序）、§五-阶段4
> （可观测性验收：/metrics 可被采集、指标产出真实数据、确认无高基数标签）。
> 冲突时以 ADR 为准，本文件随之修订。

## 1. `/readyz` 门控语义（AC1）

`/readyz` 返回 `ready`（各组件就绪布尔）+ `dependencies`（依赖健康上报项）。

- **门控集合**（仅此三项决定 200/503）：
  - `db`：数据库连接池（asyncpg，gRPC loop）；
  - `outbox_dispatcher`：outbox 投递器已启动；
  - `grpc`：gRPC server 已启动。
- **只上报不门控**：`outbox_db`（outbox 池）、`session_cache`（Redis）、
  `parse_consumer`、`rebuild_consumer`。
- **依赖健康上报项**（`_probe_dependencies`，**绝不门控**）：
  - `nats`：`_nats_client.is_connected`；
  - `rag_engine` / `milvus` / `minio`：`asyncio.gather(_probe_tcp(...))`，1s 超时。
- **理由**：NATS/rag-engine/Milvus/MinIO 宕机均属设计内降级或快速失败，
  不应把 kb-service 从 Service endpoints 摘除（ADR §五-阶段3）。

## 2. 领域指标（AC2）

注册表为 kb 私有 `CollectorRegistry`（`app/observability/metrics.py`），
经 `/metrics` 以 `text/plain; version=0.0.4` 暴露（prometheus-client）。

| 指标 | 类型 | 标签 | 语义 |
|---|---|---|---|
| `kb_outbox_undispatched_events` | Gauge | — | outbox 未投递（pending）积压量 |
| `kb_consumer_inflight` | Gauge | `consumer` | consumer 在途消息数（parse/rebuild） |
| `kb_consumer_retries_total` | Counter | `consumer` | consumer 重试计数 |
| `kb_parse_run_duration_seconds` | Histogram | `outcome` | 解析 run 时长分布（success/failure/skipped） |
| `kb_query_no_result_gate_total` | Counter | `gate` | 三道无结果闸门命中率（empty_retrieval/low_score/empty_sources） |
| `kb_degradation_total` | Counter | `component`, `reason` | 降级事件计数 |
| `kb_presign_operations_total` | Counter | `operation`, `outcome` | 预签名签发/complete 成功率（presign_put/presign_get/complete × success/failure；幂等重放单独记为 `idempotent`，不计入成功率） |

### 2.1 基数控制规则（强制）

- 标签只允许**有界枚举**：`consumer`（parse/rebuild）、`gate`、
  `component`、`reason`、`operation`、`outcome`。
- **禁止高基数标签**：`tenant_id` / `kb_id` / `doc_id` / `run_id` 等一律
  不得作为标签（阶段 4 验收项：确认无高基数标签）。

## 3. 日志关联四要素（AC3）

日志关联 `request_id` / `kb_id` / `doc_id` / `run_id` 四要素。

- **实现**：`app/observability/correlation.py` `corr(*, request_id, kb_id, doc_id, run_id)`
  返回结构化后缀 `request_id=... kb_id=... doc_id=... run_id=...`，就地拼入
  既有日志行（不引入 ContextVar/dictConfig 重构，遵守"平移优先、不做功能性
  重构"纪律）。
- **request_id 来源**：gRPC metadata `x-request-id`（Gateway 经
  `metadata.AppendToOutgoingContext` 透传）；kb 侧 `_request_id(context)` 读取，
  缺失返回空串。
- **run_id 来源**：`kb_operations` 的 task_id（解析 run 幂等键）。
- 注入点：解析编排（`parse_orchestrator`）、consumer、审计写入
  （`_record_query_audit` / `_record_query_failure_audit`）等。

## 4. 运行合同保持（AC4）

以下为现状语义，本批次**只保持不改造**：

1. **消费者先订阅后启动 dispatcher**：lifespan 内 NATS → session_cache →
   parse consumer → rebuild consumer → outbox dispatcher → gRPC server；
   停机逆序。
2. **双事件循环资源绑定**：gRPC loop（后台线程，servicer + `_db_pool` + Redis）
   与 uvicorn loop（dispatcher + consumers + `_outbox_pool`）独立；
   **跨 loop 不复用 asyncpg 连接/客户端**（asyncpg 连接绑定创建时 loop）。
3. **JetStream ack 语义**：
   - 成功 → `Ack`；
   - poison pill（不可重试）→ `Ack` 吞；
   - crash（进程崩溃/未 Ack）→ 静默重投（MaxDeliver 3）；
   - 长任务 → `InProgress` 续租（间隔 `ack_wait/3`）。
4. **优雅停机逆序**：dispatcher/consumers 先停，再关 server / 池 / 连接。

## 5. 可观测性验收（阶段 4 前置）

- `/metrics` 端点可被 Prometheus 采集（AC7）。
- 上述指标在阶段 4 全链路验收中产出真实数据（ADR §五-阶段4）。
- 确认无高基数标签。
