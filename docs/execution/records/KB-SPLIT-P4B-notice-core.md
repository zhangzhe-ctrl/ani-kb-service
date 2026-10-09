# 协作排期知会 ④ Core 团队：kb 出向 Core 调用面清零（端点下线知会）

- **发出**：2026-10-08（批次 KB-SPLIT-P4B，分支 `feat/split-kb-from-core`）
- **对象**：Core 团队（vector-stores / objects / knowledge-base-link 端点归属方）
- **依据**：`docs/adr/0001-split-kb-from-core.md` §D1（Core 依赖收缩结果）、§D3（无调用即无契约）、§四 #6、§五-阶段5（不带入清单）

## 变更内容

kb-service 在 D1 裁定（向量与对象均 kb 自管）后，出向 Core 的 REST 调用面已全部清零。`app/core_api/` 模块（`client.py` + `__init__.py`，含 vector-stores + objects + knowledge-base-link 调用面）已在本批（#065，KB-SPLIT-P4B）**物理删除**，`NotifyDocumentUploaded` 跨服务通知 RPC 同步删除（被 #058 预签名直传 + complete 端点替代）。

| 端 / RPC | 拆分前 kb 用途 | 拆分后 | 替代路径 |
|---|---|---|---|
| Core `vector-stores`（创建/删除 collection、预计算向量插入、search） | kb 经 `core_api` 调用 Core 中转向量读写 | **不再被 kb 调用** | kb 直连独立 Milvus（`app/vector_store/`，pymilvus 2.6.17；#056/#057） |
| Core `objects`（文档正本上传/取用） | kb 经 `core_api` 调 Core 对象链路 | **不再被 kb 调用** | kb 自有 MinIO bucket 预签名直传 + complete 端点 + 内网视角 URL（`app/object_store/`；#058/#059） |
| Core `knowledge-base-link` | kb 经 `core_api` 建立 KB↔vector-store 关联 | **不再被 kb 调用** | KB 与 Milvus collection 1:1（`kb_{kb_id}`，CreateKB 随建随删；#056） |
| kb.v1 `NotifyDocumentUploaded` RPC | Core 上传完成后通知 kb 登记文档 | **RPC 已删除**（proto + servicer + 调用方） | 上传唯一路径 = 预签名直传 + complete（#058） |

## 需要 Core 团队做的

1. **知悉**：上述 vector-stores / objects / knowledge-base-link 端点自本批起**不再被 kb 调用**；kb 对 Core 仅剩租户上下文信任（D2 暂缓项，`X-Tenant-Id` 等原样保留，无出向调用）。
2. **端点下线评估归属 Core**：Core 侧端点（vector-stores / objects / link）的代码下线、废弃登记或保留，归 Core 团队按自身演进节奏处置，kb 不删 Core 代码。
3. **未来新 Core 依赖按 RFC 办理**：若出现新的 Core 依赖，按 ADR §D3 流程——kb 提案（端点、动机、兼容性影响）→ Core 评审 → 发新版本 → pin 升级。流程登记见 [`docs/specs/proto-contract.md` §5](../specs/proto-contract.md)。
4. 回执本知会时确认是否有其他 kb 调用方残留或端点下线窗口约束。

## 时间窗口

- kb 侧调用面清零**已随本批完成**（#065）；Core 侧端点下线不阻塞 kb 拆分关键路径，由 Core 团队自行排期。
- 拆分期间对 Core 零出向调用不影响 Core 现有功能（其他调用方不受本变更影响）。

## 交付登记（#065，2026-10-08）

- `app/core_api/` 整模块删除；`app/core/config.py`、`app/services/contracts.py`、`app/api/grpc_server.py`、`app/consumers/parse_consumer.py` 中 Core REST base_url 配置 / core_api 引用清零。
- `NotifyDocumentUploaded` RPC 删除：`proto/kb/v1/kb_service.proto`（真源）+ Go 侧 `api/proto/kb/v1/` 镜像 + 双端 generated（`app/generated/kb/v1/*`、`pkg/generated/pb/kb/v1/*`）+ rag-engine generated 同步重生成；Gateway（`kb_resources.go` / `kb_grpc_client.go`）对应路由与 handler 删除。
- 契约层清理：`api/openapi/services/v1.yaml` 删 notify-uploaded 路径 + `NotifyDocumentUploadedRequest` schema；`architecture/services-contract-baseline.yaml` 删对应基线例外条目。
- Go 侧依赖同步确认：`grep` `pkg/generated` 与 `services/ani-gateway` 对 `NotifyDocumentUploaded` 零匹配——Go 侧无残留引用，无需额外知会项。
- 网络层验证（AC）：kb 进程无 Core Service 出向连接配置（`grep` kb-service 对 `core_api|CoreClient|CORE_API_BASE|core_service_url|CORE_SERVICE_URL` 零匹配）。

## 回执记录

| 日期 | 回执方 | 结论 |
|---|---|---|
| 2026-10-08 | — | 知会发出，待回复 |
