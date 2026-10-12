# 协作排期知会 ① 前端：文档上传改为 kb 签发预签名直传（裁定 D1）

- **发出**：2026-09-21（批次 KB-SPLIT-P0，基线 commit `68bf198f`）
- **对象**：前端团队（Console 知识库模块）
- **依据**：`docs/adr/0001-split-kb-from-core.md` §二-D1（对象自管 + 预签名直传）、§五-阶段1（存储基线）、阶段2（上传契约固化）

## 变更内容

| 项 | 现状 | 拆分后 |
|---|---|---|
| 上传通道 | 前端调 Core 上传端点（objects 链路），Core 通知 kb（NotifyDocumentUploaded RPC） | 前端经 Gateway 请求 **kb 签发预签名 PUT URL**（MinIO NodePort 对外地址、时效受限、key 预定）→ **前端直传 MinIO** → 前端调 **kb complete 端点**（HeadObject 校验 + 登记 kb_documents） |
| 相关端点 | Core vector-stores / objects / knowledge-base-link | 上述 Core 端点不再出现；上传/取用契约全部由 kb 提供 |
| 上传体验 | 两步（上传 + 等待） | 三步（请求签名 → 直传 → complete），对用户单次上传语义不变 |

## 需要前端做的

1. 上传逻辑改造：从「调 Core 上传端点」改为「kb 签发预签名 → 直传 → complete」三段式（**阶段 2 上传契约固化后按新契约接入**）。
2. 关注契约项：预签名 URL 时效、key 规则、双视角 URL（对外 NodePort / 对内 ClusterIP）——阶段 2 交付并列入契约测试。
3. 回执本知会时反馈前端改造排期窗口。

## 时间窗口

- kb 侧存储基线（阶段 1，issue #058/#059）建成即具备后端能力；阶段 1 完成前现状不变、无行为差异。
- 前端改造不阻塞 kb 拆分关键路径（阶段 2 依赖前端排期确认，阶段 4 全链路验收含预签名直传专项）。

## 回执记录

| 日期 | 回执方 | 结论 |
|---|---|---|
| 2026-09-21 | — | 知会发出，待回复 |

---

## 补充（2026-10-12）：501 `Method not found!` 定性 + 最终三步契约

### 现场现象

前端上传文档时，`POST /api/v1/svc/knowledge-bases/{kb_id}/documents/{doc_id}/notify-uploaded`
（经 `ani-console` NodePort **30081** 代理到 `ani-gateway`）返回 **501 Not Implemented**，
body `{"message": "Method not found!"}`。

### 定性结论：后端源码无缺陷，是**线上 gateway 版本落后**

三重证据：

1. **源码侧**：`ani-gateway` HEAD 的 [kb_resources.go](../../../../repo/services/ani-gateway/internal/router/kb_resources.go) **已无**
   `notify-uploaded` 路由与 handler（`4bc7ce09`，#065 删除）；`ani-kb-service` HEAD 的
   [kb_service.proto](../../proto/kb/v1/kb_service.proto) **已删** `NotifyDocumentUploaded` RPC。
   gRPC Python 对未实现方法返回默认 `b"Method not found!"`，经
   [kb_grpc_client.go `mapGRPCError`](../../../../repo/services/ani-gateway/internal/router/kb_grpc_client.go#L497-L513)
   `Unimplemented → 501` 透传，故出现 `501 / Method not found!`。
2. **线上 gateway 实测（NodePort 30080 直连）**：
   - `POST .../notify-uploaded` → **401**（路由**存在**，被鉴权拦截，说明仍是旧版本注册了旧路由）
   - `POST .../documents/{doc_id}/complete` → **404**（新路由**不存在**）
   - `POST .../documents` → 401（存在）
3. **镜像版本**：线上 `ani-gateway` 运行 `docker.changqingyun.cn/ani/ani-gateway:test2-20261009-vmstart`
   （Oct 9 构建），**落后于** KB 拆分切换（`4bc7ce09` 于 10-08 删旧路由、`c7c119ae` 于 09-28 加三步流）。
   线上 `kb-service` 已是 `kb-20261012-head-a5f8fe9-r2`（新）。

> 结论：**不是后端 HEAD 的问题**。旧 gateway 把 `notify-uploaded` 转发给新 kb-service，
> 新 kb-service 无此 RPC → 501。前端必须改新契约，**且 gateway 必须重新构建/部署**到含
> `/complete` 的版本，否则新契约前端仍会 404。

### 上传契约对照（旧 → 新）

| 步骤 | 旧（Core 时代，已废弃） | 新（kb 自管，ADR §D1） |
|---|---|---|
| 1 | `POST /knowledge-bases/{kb_id}/documents`（Core 上传） | `POST /knowledge-bases/{kb_id}/documents`（`getDocumentUploadURL`）→ 200 `{doc_id, upload_url, storage_path}` |
| 2 | `PUT MinIO`（Core 代传） | 前端**直传** MinIO：`PUT {upload_url}`（presigned，**15 分钟有效**） |
| 3 | `POST .../documents/{doc_id}/notify-uploaded`（**已删除**） | `POST .../documents/{doc_id}/complete`（`completeDocumentUpload`）→ **202** `{task_id, task_type, status}` |

### 前端需要的修改

1. **删除**对 `.../notify-uploaded` 的调用（该端点在拆分后不复存在，任何情况下都会 501）。
2. **step 1 改为**：`POST /knowledge-bases/{kb_id}/documents`，body：
   `{idempotency_key(uuid, 必填), file_name(必填), file_type(必填), file_size_bytes, checksum_sha256, custom_metadata}`；
   响应 200 `{doc_id, upload_url, storage_path}`。
3. **step 2**：对 `upload_url` 执行 `PUT`（原始二进制，15 分钟内完成；无需带 gateway token）。
4. **step 3 改为**：`POST /knowledge-bases/{kb_id}/documents/{doc_id}/complete`，body：
   `{idempotency_key(uuid, 必填), file_name(必填), file_type(必填), checksum_sha256, custom_metadata}`；
   `doc_id` 取自 step 1 响应，**走路径**；响应 202 `{task_id, task_type, status}`，用 tasks API 轮询解析结果。
5. **鉴权**：三步均需携带既有 gateway 鉴权头（Bearer/ApiKey）；401 即未带/失效。
6. 契约真源：[api/openapi/services/v1.yaml](../../../../repo/api/openapi/services/v1.yaml#L2704-L2780)。
7. **部署前置**：需先把 gateway 重新构建并部署到含 `/complete` 的版本（当前线上为旧构建）。
