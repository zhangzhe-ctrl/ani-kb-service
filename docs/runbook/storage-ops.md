# kb-service 存储运维手册（KB-SPLIT-P3C / issue #063）

> 本文件是 kb-service 拆分事务中**存储运维归属落地（D1）**的落地说明：
> 自管 Milvus、自管 bucket 的监控/备份/容量/生命周期责任，以及预签名
> URL 时效配置。执行期权威裁定见 [ADR 0001](../adr/0001-split-kb-from-core.md)
> §五-阶段3（存储运维归属落地：Milvus 监控/备份/容量，含 etcd 配额与
> auto-compaction 配置教训；bucket 生命周期策略与配额；预签名 URL 时效配置）
> 与 §七-后果（代价：kb 承接 Milvus 与 bucket 的运维责任）。
> 冲突时以 ADR 为准，本文件随之修订。
>
> 本手册不含真实凭据；凭据由 K8s Secret 注入（`.env.example` 占位符）。

## 0. 组件拓扑与服务器地址

拆分后 kb-service 直连以下自管组件（D1），不再经 Core 中转。现网已运行
组件与地址见 `real-k8s-lab/components.md`，服务器基线见
`dev-physical-servers(2).md`。

| 组件 | 归属 | 现网地址（示例） | kb 配置项 |
|---|---|---|---|
| Milvus（向量库） | kb 自管（D1） | `10.10.1.66:31930`（NodePort，无鉴权） | `MILVUS_ADDR` / `MILVUS_TOKEN` / `MILVUS_TIMEOUT` |
| MinIO（对象桶） | kb 自管（D1） | `10.10.1.66:30900` | `MINIO_ENDPOINT` / `MINIO_PUBLIC_ENDPOINT` / `MINIO_BUCKET` / `MINIO_PRESIGN_TTL_SECONDS` |
| PostgreSQL | kb 专属实例 | 共享库 NodePort `30945`；kb 专属 `30946` | `DATABASE_URL` |
| NATS JetStream | 共享（沿用 `ani.tasks.kb.*`） | `31062` | `NATS_URL` |
| Redis | 共享（会话缓存） | `30453` | `REDIS_URL` |

服务器（dev-phys，见 `dev-physical-servers(2).md`）：

| 主机 | 地址 | Tailscale | 备注 |
|---|---|---|---|
| ANI1 / dev-phys-01 | `10.10.1.66` | `100.64.128.1` | 承载 Milvus（`ani-s06-vectorstore`）、MinIO |
| ANI2 / dev-phys-02 | `10.10.1.67` | `100.64.128.2` | 数据盘 3.5T（部署前先明确数据盘规划） |
| ANI3 / dev-phys-03 | `10.10.1.68` | `100.64.128.3` | 数据盘 3.5T（部署前先明确数据盘规划） |

> 数据盘规划：`dev-physical-servers(2).md` 明确"部署 Milvus/MinIO/Harbor
> 前先明确数据盘规划"，系统盘 893.8G / 数据盘 3.5T 未挂载时不得投入生产。

## 1. Milvus 运维（AC5）

### 1.1 监控

- **实例命名空间**：`ani-s06-vectorstore`（现网示例，见 `real-k8s-lab/components.md`）。
- **健康探针**：kb 进程侧在 `/readyz` 上报 Milvus 健康
  （`_probe_tcp`，0.3s 超时；刻意低于 readinessProbe 默认 1s，避免上报项被
  误升级为门控）；**Milvus 健康不进入 `/readyz` 门控**——Milvus
  宕机属设计内可重试/快速失败，仅作上报项（ADR §五-阶段3-运行合同）。
- **超时兜底**：`MILVUS_TIMEOUT`（默认 10s，`config.py`）。D1 直连后 kb 与
  Milvus 之间无中间层超时兜底，不传 timeout 会无限等待；Milvus/etcd 挂起时
  拖死 Query/parse。**务必保持非零超时配置。**
- **建议监控项**：实例存活、gRPC 端口连通（NodePort `31930`）、collection
  数量与 `kb_*` collection 行数、搜索/插入 P99 延迟、etcd 后端磁盘占用。

### 1.2 etcd 配额与 auto-compaction 教训（必须继承）

现网曾发生 etcd 磁盘配额耗尽导致 Milvus 不可写。证据与处置脚本：
`real-k8s-lab/fix_milvus_etcd_nospace.sh`、`real-k8s-lab/etcd_autodefrag_cronjob_0929.yaml`。

- **配额**：etcd 默认配额 `QUOTA=8589934592`（8 GiB）。
- **触发阈值**：磁盘占用达 **60%（约 5.15 GiB）** 即需处置，否则后续写
  入被拒绝（`etcdserver: mvcc: database space exceeded`）。
- **自动处置**：CronJob `etcd-auto-defrag`（命名空间 `ani-s06-vectorstore`），
  schedule `17 */6 * * *`（每 6 小时），超阈后依次执行 compact + defrag +
  alarm disarm。
- **手动处置流程**（`fix_milvus_etcd_nospace.sh`）：
  1. `etcdctl endpoint status` 查看各 endpoint 的 dbSize / dbSizeInUse；
  2. `etcdctl compact <rev>` 压缩历史版本；
  3. `etcdctl defrag --command-timeout=300s`（**必须给足 300s**，defrag 期间
     实例短暂不可写）；
  4. `etcdctl alarm disarm` 解除 no-space 告警。
- **教训**：仅靠 auto-compaction 不足以控制配额；必须叠加 defrag 与
  alarm disarm 的定时任务，且 defrag 命令超时要放宽到 300s，避免中途被
  客户端超时中断留下半完成状态。

### 1.3 备份

- **collection 级**：按 kb 语义，向量可**由文档正本重建**——换而言之
  Milvus 不是唯一真源，正本在 kb 自有 bucket（见 §2）。灾备优先策略：
  1. 备份 kb 专属 PostgreSQL（B 层表 + `kb_outbox_events` + `kb_operations`），
     这是所有权与元数据的真源；
  2. 备份 kb 自有 bucket（文档正本 + 图片产物）；
  3. Milvus collection 可在恢复后用 rebuild 流程（`ani.tasks.kb.rebuild.v1`）
     从正本 + PG 元数据重建，无需逐条备份向量。
- **重建入口**：整库 rebuild 事件由 outbox dispatcher 路由到
  `ani.tasks.kb.rebuild.v1`，rebuild consumer 承接（`kb_rebuild_consumer_enabled`）。

### 1.4 容量

- collection 名 `kb_{kb_id}`，随 CreateKB 建立、DeleteKB 删除（best-effort）。
- 维度由 embedding 模型实测决定（`embedding_dim` 仅探测失败回退值）。
- 容量增长 = Σ(kb 文档数 × chunk 数 × 向量维度 × 4B) + 索引开销。
- **Milvus 数据持久化依赖 etcd + 对象存储后端（现网 minio）+ PVC**；扩容前
  先确认 §1.2 的 etcd 配额余量。

## 2. bucket 生命周期策略与配额（AC6）

- **物理桶**：`ani-kb-docs`（`config.py` `minio_bucket`）。**kb 进程不自行建桶**，
  桶由部署侧创建（compose/K8s `mc mb --ignore-existing` 幂等行）。
- **key 规则（正本）**：
  - 文档正本：`{kb_id}/{doc_id}`；
  - 解析期图片产物：`{kb_id}/{doc_id}/images/{uuid}.{ext}`。
- **生命周期建议**：
  | 前缀 | 建议策略 |
  |---|---|
  | `{kb_id}/{doc_id}`（正本） | 随 DeleteDocument/DeleteKB 由 kb 清理（best-effort）；不设自动过期，正本是重建真源 |
  | `{kb_id}/{doc_id}/images/` | 随文档删除按前缀批量清理（`remove_objects_by_prefix`） |
  | 未登记对象（presign 后未 complete） | 建议 Object Lifecycle Rule：`Expiration` 24h，回收孤儿上传 |
- **删除语义**：`_delete_document` 删正本 key + 图片前缀；`_delete_kb` 按
  `{kb_id}/` 全前缀清理，衔接 #053 `delete_kb_cascade` 的 CASCADE 语义。
  删除为 **best-effort**：对象存储故障只 warning 不阻断 DB 侧删除。
- **配额**：在 MinIO 侧对 `ani-kb-docs` 设置桶配额（容量上限）与告警；
  配额值随数据盘规划（`dev-physical-servers(2).md`）确定，避免占满共享数据盘。
- **镜像/插件类对象**（Harbor 等）不属 kb bucket，另计。

## 3. 预签名 URL 时效配置项化（AC6）

- **配置项**：`MINIO_PRESIGN_TTL_SECONDS`（`config.py` `minio_presign_ttl_seconds`），
  默认 **900 秒（15 分钟）**，先例同 Core Go，SDK 上限 7 天。
- **签发范围**：
  - presign PUT（上传直传）：仅授 `PUT` 单操作，TTL = 配置值；
  - presign GET（对象取用，`GetObjectURL`）：TTL = 配置值，返回
    `{url, expires_seconds}`。
- **非正值拒绝**：本地拒绝非正 TTL（`presigned_put_object`），避免签发
  无时效或已过期 URL。
- **双视角 URL**：presign 走 `MINIO_PUBLIC_ENDPOINT`（NodePort 对外，
  如 `10.10.1.66:30900`）；stat/HeadObject 走 `MINIO_ENDPOINT`（ClusterIP 对内）。
  `MINIO_PUBLIC_ENDPOINT` 为空串时 fallback 内网视角（单节点/dev）。
- **时效与越权测试面**：过期 URL 拒绝、key 越权（bucket/key 前缀双闸）在
  阶段 4 隔离验证的 D1 专项中验收（ADR §五-阶段4）。

## 4. 与运行合同的关系（AC1/AC4）

- 本手册涉及的 Milvus / MinIO **均不进入 `/readyz` 门控**；其健康作为
  `/readyz` 的 `dependencies` 上报项（NATS / rag-engine / Milvus / MinIO）。
  门控集合恒为 db pool + outbox dispatcher + gRPC server（ADR §五-阶段3）。
- 存储组件不可用时的语义：
  - Milvus 不可用 → 检索/写入快速失败（可重试错误）；
  - MinIO 不可用 → presign/complete 返回可重试错误；删除 best-effort；
  - 均**不阻断进程存活**，降级事件计入 `kb_degradation_total`（见可观测性）。
- 存储相关领域指标（outbox 积压、run 时长、presign 成功率等）见
  `docs/specs/observability.md`。

## 5. 参考

- `real-k8s-lab/components.md`：现网已运行组件与地址。
- `real-k8s-lab/fix_milvus_etcd_nospace.sh`：etcd 配额耗尽手动处置。
- `real-k8s-lab/etcd_autodefrag_cronjob_0929.yaml`：etcd 定时 defrag CronJob。
- `dev-physical-servers(2).md`：服务器地址与硬件/数据盘基线。
- [ADR 0001](../adr/0001-split-kb-from-core.md) §五-阶段3、§五-阶段4、§七。
