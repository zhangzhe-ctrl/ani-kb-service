# kb-service 拆分 — 执行状态

> 本文件是 kb-service 拆分事务执行状态的唯一真实来源。每批次完成后更新「当前阶段」；过程细节与知会原文存 `records/`。导航入口：[../START-HERE.md](../START-HERE.md)。

## 拆分基线（阶段 0，批次 KB-SPLIT-P0，2026-09-21 固定）

| 项 | 值 |
|---|---|
| 分支 | `feat/split-kb-from-core` |
| 快照 commit | `68bf198f9c31a55390222daed05d4aec257ff37f`（2026-09-21 13:44:49 +0800，"Merge remote-tracking branch 'upstream/main' into feat/kb-api-completion"） |
| 固定日期 | 2026-09-21 |
| ADR | [adr/0001-split-kb-from-core.md](../adr/0001-split-kb-from-core.md)（rev2；执行期唯一权威文本，plan 模块同名文件为规划期历史稿，见 START-HERE 冲突裁决） |
| 拆分 issue 清单 | monorepo `repo/services/tasks/modules/issue/core/knowledge/`（#049-#070） |

### 冻结纪律（阶段 0 落档，拆分全程有效）

- **拆分期间平移优先、不做功能性重构。** 现状行为（状态机、幂等、降级、ack 语义）原样平移；任何功能性重构、引擎更换（pgvector 评估）、身份机制调整（D2）均属演进期，须另立 ADR（ADR 0001 §1.5、§三-4、§六）。
- 首发即目标态：无停写窗口、无数据迁移、无停用序列、无回滚预案；旧路径与旧依赖以「代码不带入」方式消失（ADR 0001 rev2 修订说明）。
- 违反本纪律的改动不得进入拆分分支。

### 基线测试结果（快照 commit 下的全量门禁，2026-09-21 执行）

| 命令 | 结果 | 备注 |
|---|---|---|
| `make validate-architecture` | ✅ 通过 | "architecture guardrails valid"（component import guard + inference legacy control plane + auth contract + gateway authz：18 tests OK、drift 0、route coverage 323 registered / 249 registry / 0 error） |
| `make test-go`（等价直跑） | ✅ 全部包通过（1 项预存环境失败） | 全部 Go 包 `ok`；仅 `pkg/adapters/runtime` 2 个沙箱 symlink 测试 FAIL——`CURRENT-SPRINT.md`（Gateway 鉴权 PR5 章节）已记录的 Windows 预存失败（sandbox symlink 特权，origin/main 同 FAIL），不在本批次改动集 |
| `make test-python`（等价直跑） | ✅ 仓库源码 0 错误 | rag-engine 仓库 25 个 .py 编译 0 错误；`make` 原命令在本机被 `ai/rag-engine/.venv`（gitignore 的本地工件，内含 torch py312 语法文件）触发失败，预存环境问题，与本批次无关 |
| `git diff --check` | ✅ 通过 | 本批次仅新增/修改文档 |

> 执行说明：`make test` 顶层目标在本机沙箱内因 Windows 环境限制（`GOCACHE=` 内联环境变量语法不被识别、`/dev/null` 重定向受限）无法整体执行，故按 Makefile 逐目标等价直跑；validate-architecture / auth-contract / gateway-authz 三道校验在 `make test` 链内原生通过（2026-09-21 日志）。

## 现状耦合差异清单（ADR §四 10 项 × 处置 issue）

> 按快照 commit `68bf198f` 现状盘点；「处置 issue」为 monorepo 本地 issue 编号（`issue/core/knowledge/issue-0NN-*.md`）。全部状态在处置批次合入前保持「待处置」。

| # | 耦合点 | 现状证据（快照 commit） | 处置 issue | 状态 |
|---|---|---|---|---|
| 1 | 共享数据库 | `app/core/config.py` `database_url` 默认连 monorepo 共享库 | #055 KB-SPLIT-P1F（kb 专属 PG 实例 + 角色体系 + 跨租户负向测试） | 已处置（#055，2026-09-23：`deploy/docker/docker-compose.kb-pg.yml` 建 kb 专属 PG 17 实例 127.0.0.1:5433/kb，与共享实例 5432/ani 并存互不影响，init-scripts 只初始化角色/密码、表结构唯一真源为迁移基线重放；`config.py` `database_url` 默认值改指 `ani_app_user@localhost:5433/kb`，不再默认共享库；`migrations/000_kb_dedicated_roles.sql` 三角色最小权限体系（ani_app NOLOGIN 组 / ani_app_user LOGIN 应用运行时唯一身份 / ani_migrator 迁移运维，全部 NOSUPERUSER/NOCREATEDB/NOCREATEROLE/NOBYPASSRLS，无 platform_bypass 类超级角色）+ `migrations/014_kb_sequence_grants.sql` 补 BIGSERIAL 序列 USAGE/SELECT（009 kb_outbox_events.id 的应用角色 INSERT 前置）；003-007 零重放修复（DO $$/to_regclass 守卫，P1D/P1E 登记的重放阻塞点清零）——000-014 在全新库按文件名序一次跑通零错误零重放；`tests/test_dedicated_pg_cross_tenant.py` 28 项真实 PG 跨租户负向测试（KB_TEST_ADMIN_DATABASE_URL env 门控，未设 28 skipped 保持 make test 绿）：admin 建一次性库从零重放后以 ani_app_user 身份执行全部断言——A+B 层 + kb_permissions/kb_audit_log/kb_operations/kb_outbox_events 全资源，另一租户 GET/UPDATE/DELETE 均 None/空集/False/0/默认值且 B 行二次读取存活；outbox 按 009 语义验证 dispatcher 刻意跨租户轮询。**阶段 1 验收证据三态：pass**——真实远程 PG 28/28 全绿 + 独立库全量单测 562 passed + make validate-architecture 三脚本 + `git diff --check` 通过。注：共享库部署未动，其他服务仍用共享库；专属实例与独立环境全链串联已在 #066（P4C）完成 ✅ 2026-10-09——独立环境下 kb 专属 PG `10.10.1.66:30946` 9 表无 RLS 运行时时确认、全链路验收 e2e PASS 58/FAIL 0/NOT_VERIFIED 4） |
| 2 | 表 DDL 所有权分裂 | A 层 3 表在 kb `migrations/001-007`；B 层 4 表（knowledge_bases/kb_documents/kb_sessions/kb_messages）DDL 在共享库 `deploy/migrations/init_schema.sql`；C 层 2 表（async_tasks/outbox_events）多写入方 | #053 KB-SPLIT-P1D（B 层 DDL 纳入基线）；#050 KB-SPLIT-P1A（kb 自有 outbox） | outbox 部分已处置（#050，2026-09-21：`migrations/009_kb_outbox.sql` 建 kb 自有 `kb_outbox_events`，repository/dispatcher 切换）；B 层 DDL 已处置（#053，2026-09-22：`migrations/011_kb_b_layer_tables_baseline.sql` 重建 B 层 4 表完整 DDL 纳入 kb 基线——以 repositories 实际读写为准核对列/索引/约束，合并共享库 4 项增量 ALTER（kb_name_unique_active / kb_sessions_kb_id_index / kb_embedding_model_full_name / kb_default_inference_service）+ kb 侧 003/004/007 增量与 custom_metadata/object_id 运维列；grep 验证 app/ 内无 CREATE/ALTER TABLE，migrations/ 为唯一 DDL 来源） |
| 3 | 跨库 FK 断裂 | `knowledge_bases.tenant_id REFERENCES tenants(id) ON DELETE CASCADE` | #053 KB-SPLIT-P1D（去 FK + 应用层租户校验 + 删除流程承接 CASCADE 语义） | 已处置（#053，2026-09-22：迁移无任何共享库对象引用；CreateKB 租户校验降级应用层——信任 Gateway 已认证租户上下文（D2 边界收敛），保留 UUID 格式校验；`delete_kb_cascade` 接管 ON DELETE CASCADE 语义——事务内按 FK 依赖序硬删 kb_messages/kb_sessions/kb_documents + KB 主行软删（保 kb_audit_log 审计历史，部分唯一索引释放名称）；向量/对象清理归 #056/#058） |
| 4 | RLS + GUC 租户隔离 | `app/repositories/rls.py` `SET LOCAL app.current_tenant_id` + platform_bypass + `ani_app` 角色 | #054 KB-SPLIT-P1E（显式过滤 + 复合外键 + 独立库角色体系） | 已处置（#054，2026-09-23：`migrations/012_kb_explicit_tenant_isolation.sql` DROP POLICY × 13 + NO FORCE/DISABLE ROW LEVEL SECURITY × 9 表 + 清 005/006 遗留跨库 FK，内部父子关系改复合外键 `(tenant_id, id)` × 6（kb_documents/kb_sessions/kb_permissions/kb_audit_log→knowledge_bases、kb_messages→kb_sessions、kb_chunks→kb_documents，kb_chunks 原本无 FK 一并补齐）；`rls.py` 删 GUC 退化为 validate_tenant_id 过滤助手；9 个 repository 模块全部查询显式 tenant_id 过滤（retrieve_service 裸 SQL 下沉 repository）；SQL 集中门禁 `scripts/validate_kb_sql_centralization.py` 入 `make validate-architecture`（kb 表 SQL 只许在 `app/repositories/`、app/ 内禁 DDL/RLS/GUC 复活）；39 项跨租户负向测试全资源覆盖；533 passed + validate-architecture + `git diff --check` 通过。注：独立库角色体系属 #055（P1F），本条仅 RLS→显式隔离收敛） |
| 5 | 共享 repo 根 `.env` | `app/core/config.py` `env_file=".env"` 指向 monorepo 根；`embedding_model`/`embedding_dim` 为共享键 | #061 KB-SPLIT-P3A（自建 `.env.example` + 键专属命名 + Secret 化） | 已处置（#061，2026-10-08：`config.py` `env_file` 由相对 `.env`（随 CWD 命中 monorepo 根）改绝对路径 `Path(__file__).resolve().parents[2] / ".env"`（kb-service 仓库根，不再依赖 CWD、不读 monorepo 根 `.env`）；`embedding_model`/`embedding_dim` 字段名不变、环境变量键改 `KB_EMBEDDING_MODEL`/`KB_EMBEDDING_DIM` + `AliasChoices` 兼容旧共享键（kb 专属键优先）；**无默认向量模型**——`embedding_model` 默认空串，CreateKB 请求体与 env 均未配置时返回 `INVALID_ARGUMENT`（收口期 human-gate override）；`embedding_dim` 保留 1024 作维度探测失败回退（非维度真源）；新建 `services/kb-service/.env.example`（全部配置项分组 + 中文说明 + 敏感项占位符 `<PG_PASSWORD>`/`<MINIO_*>`、`MILVUS_TOKEN` 留空，文件头注明生产由 K8s Secret 注入）；`requirements.txt` `jieba` 补钉 `==0.42.1` 全依赖固定。本批不新建 kb 专属 K8s 清单（无独立 Deployment，Secret 注入随 P5 迁出落地）。645 passed + 28 skipped + validate-architecture + `git diff --check` 通过） |
| 6 | Core REST 调用面 | `app/core_api/client.py`（vector-stores + objects + link）；NotifyDocumentUploaded 跨服务通知 | #056/#057 KB-SPLIT-P1G/P1H（向量自管直连 Milvus）；#058/#059 KB-SPLIT-P1I/P1J（对象自管 bucket + 预签名直传）；#065 KB-SPLIT-P4B（core_api 模块与 NotifyDocumentUploaded 删除） | 向量部分已处置（#056，2026-09-23：`app/vector_store/` pymilvus 2.6.17 AsyncMilvusClient 直连独立 Milvus——显式 schema id VARCHAR/FLOAT_VECTOR(dim)/content VARCHAR/metadata JSON + AUTOINDEX COSINE、create 幂等/drop/recreate 生命周期、insert/search/delete 数据面（输出保持 Core searchVectorStore items 形状）、懒连接进程单例、VectorStoreError 包装；`config.py` milvus_addr/milvus_token（MILVUS_ADDR 与 rag-engine 同键，token Secret 归 #061）；CreateKB 探测维度建 `kb_{kb_id}` collection + `set_vector_store_id` 落 collection 名（vector_store_id 列语义平移 Core id → Milvus collection 名，下游消费者签名零改动）；DeleteKB best-effort drop；DeleteDocument/reparse `metadata["doc_id"]` 过滤删；retrieve search 直连；rebuild 前 recreate（维度探测回落 EMBEDDING_DIM，失败 fail-fast）；requirements 钉死 pymilvus==2.6.17；CoreClient vector-store 方法成死代码待 #065 清理）；向量路径收口已处置（#057，2026-09-24：`tests/test_vector_roundtrip.py` 6 项测试——`_RoundtripVectorStore` 带真实余弦排序的 fake 同一实例注入 parse 写侧 + retrieve 读侧构成真往返：写入-检索往返（命中集合/排序/score 透传）、summary backfill、hybrid 同 store（keyword 腿与 RRF 融合不变）、重解析 delete 先行（`ops==["delete","insert","delete","insert"]` + 旧向量无残留）、delete doc-scoped（不误删他文档）、delete 后检索空；正交单位向量零浮点容差；全量 565 passed + 28 skipped（P1G B' 后基线 559 + 6 零回归）；grep 证据：`core_client.(insert|delete|search|create|list)_vector` services/ 零命中（contracts.py 仅 Protocol 声明、client.py 死代码归 #065）；生产代码零改动纯收口）；对象自管已处置（#058，2026-09-24：`app/object_store/` MinioObjectStore（minio==7.2.15 同步 SDK + asyncio.to_thread 桥接）——双视角双客户端（presign 走 `minio_public_endpoint` NodePort 对外、stat 走 `minio_endpoint` ClusterIP 对内、public_endpoint 空串 fallback 内网覆盖单节点/dev）、`presigned_put_object` 仅授 PUT 单操作 + TTL 配置化默认 900s 非正数本地拒绝、`stat_object` HeadObject 语义、NoSuchKey 二分（对象确实不存在 → ObjectNotFoundError/FAILED_PRECONDITION 拒绝登记 vs 网络故障 → ObjectStoreError/UNAVAILABLE 可重试）、懒构造进程单例；grpc_server presign 端点重写（成功路径零 DB 写，audit 与登记全部移到 complete，D1 三步流审计契约，key 预定 `{kb_id}/{doc_id}` 正本规则）+ complete 端点新增（HeadObject 校验对象存在与非零 size → 单事务四写 kb_documents pending → kb_operations task → kb_audit_log → kb_outbox parse 触发解析；UNIQUE 幂等回放、同 doc 重复 ALREADY_EXISTS、对象不存在/空对象拒绝登记且零审计）；proto CompleteDocumentUpload RPC + Request 八字段 + NotifyDocumentUploaded 标 DEPRECATED（删除归 #065，新链路零依赖）；config 8 个 MINIO_*（secure 用 AliasChoices 认 MINIO_SECURE/MINIO_USE_SSL 双键）+ requirements minio==7.2.15；物理桶 `ani-kb-docs` 部署侧 compose `mc mb --ignore-existing` 既有幂等行（kb 进程不自行建桶）；28 项新测试 + 3 既有文件断言迁移，全量 593 passed + 28 skipped（P1H 基线 565 零回归）+ validate-architecture 4 guardrail + `git diff --check` 通过）；对象生命周期与 Gateway 代理路由已处置（#059，2026-09-28：新增 kb.v1 `GetObjectURL` RPC（`{tenant_id, kb_id, object_id}` → `{url, expires_seconds}`）——object_id 语义为 kb 自管对象标识 `{bucket}/{key}`，handler 原地解析（`partition("/")` 拒畸形）+ 双闸归属校验（bucket 必须等于 kb 自有 bucket、key 必须前缀 `{kb_id}/`，否则 FAILED_PRECONDITION→409）+ kb 存在性校验（NOT_FOUND→404）+ 内网 client 签发 presigned GET（UNAVAILABLE→503），供解析管线取文档正本与前端按 object_id 查图直连 MinIO；`MinioObjectStore` 新增 `remove_object`（S3 DELETE 幂等）+ `remove_objects_by_prefix`（list 后逐删并计数）+ `bucket`/`presign_ttl_seconds` property；`grpc_server._delete_document` 接入 best-effort 对象清理（正本按 `storage_path` key 删 + 图片按 `{kb_id}/{doc_id}/images/` 前缀批量删，故障只 warning 不阻断）+ `_delete_kb` 按 `{kb_id}/` 全前缀清理（衔接 #053 `delete_kb_cascade` CASCADE 语义）；`parse_orchestrator` 解析期图片产物直连上传 kb bucket（key `{kb_id}/{doc_id}/images/{uuid}.{ext}`，markdown 占位 `[图片: caption](object_id)` 机制保留、object_id 即 `{bucket}/{key}`）；Gateway 纯 gRPC 代理新增 `kb_grpc_client.go` `CompleteDocumentUpload`/`GetObjectURL` + `kb_resources.go` 两条路由 `POST /knowledge-bases/:kb_id/documents/:doc_id/complete`（202 AsyncTask）与 `GET /knowledge-bases/:kb_id/objects?object_id={bucket}/{key}`（200 `{url, expires_seconds}`，缺参 400、object 非本 KB 409、nil client 503），OpenAPI `services/v1.yaml` contract-first 同步两 path + schema 并重生成双端生成物，route baseline 确认无需新增例外；`test_object_url.py` 10 项 + 删除清理断言 + object_store 6 项 + Gateway 两端点用例，kb-service 615 passed + 28 skipped（P1I 基线 593 零回归）+ Gateway 全包 go test ok + `make validate-services-route-contract`（9 accepted / 0 error）+ validate-architecture 4 guardrail + `git diff --check` 通过）；core_api 模块与 NotifyDocumentUploaded 删除已处置（#065 KB-SPLIT-P4B，2026-10-08：`app/core_api/` 整模块删除（CoreClient + CoreAPIError）；`config.py` 删 `ani_gateway_internal_url`/`core_api_base_path` 字段与 `core_api_base_url` property；`contracts.py` 删 `CoreClientProtocol`；`grpc_server.py` 删 core_api import + `core_client_factory` 注入 + `_notify_document_uploaded`/`NotifyDocumentUploaded`；proto 双端删 RPC + `NotifyDocumentUploadedRequest` 并重生成，RPC 面 28→27；Gateway `kb_resources.go`/`kb_grpc_client.go` 删 notify-uploaded 路由与方法；OpenAPI `services/v1.yaml` 删 notify path/schema、baseline 例外清零、allowlist 注释改 CompleteDocumentUpload；`.env.example` 删两项；SDK/API docs 重生成幂等；对视 Core 仅剩租户上下文信任（X-Tenant-Id 原样）、出向 Core HTTP 调用清零（grep kb-service app/ 零匹配）；Core 端点下线知会 ④ 发出落档 `records/KB-SPLIT-P4B-notice-core.md`；验证 627 passed + 28 skipped + validate-architecture 4 guardrail + route contract 0 error + `git diff --check` exit 0） |
| 7 | proto 生成物三份复制 | `app/generated/kb/v1/`、`app/generated/common/v1/`、`app/rag_engine/rag_pb2*` | #060 KB-SPLIT-P2（pin 版本统一分发 + 契约测试） | 已处置（#060，2026-09-28：proto 源收敛为 kb-service `proto/` 单一真源（`proto/kb/v1/kb_service.proto`、`proto/common/v1/common.proto`、`proto/rag/v1/rag.proto`），三对镜像（Go 侧 `api/proto/kb|common/v1/`、rag-engine 侧 `ai/rag-engine/app/grpc/rag.proto`）与真源 **sha256 逐字节一致（pin）**；pin 约定 + Python 生成命令（版本锁定 protobuf==7.35.1 / grpcio==1.83.0 / grpcio-tools==1.83.0）+ rag_pb2_grpc.py 相对导入修正 + D2/D3 边界落 `docs/specs/proto-contract.md`；`tests/test_proto_contract.py` 22 项契约测试（pin 双侧一致 / 生成物↔proto 源一致性 / rag 服务面 + EmbedResponse 扁平 float 数组字段号与 wire 往返 / 上传三步流时效-key-双视角 / D2 租户上下文传递 / RPC 面 28 全量）；阶段 2 只契约化与 pin，**不删除**生成物（删除属阶段 5）；全量 639 passed + 28 skipped + validate-architecture + `git diff --check` 通过） |
| 8 | 双解析路径并存 | flag `kb_parse_consumer_enabled` 门控 v1（rag-engine parse_worker）/v2（kb consumer）；OFF 时发 v1 subject | #064 KB-SPLIT-P4A（v1 路径 + flag 门控 + OFF 发 v1 逻辑删除，v2 为唯一路径） | 已处置（#064，2026-10-08：删除 `config.py` `nats_parse_subject`（v1 subject `ani.tasks.kb.parse`）与 `kb_parse_consumer_enabled` 字段；`main.py` parse consumer 由 flag 门控改为**无条件启动（常开）**、subject 固定 `settings.nats_parse_subject_v2`，dispatcher/`outbox/dispatcher.py` 默认 subject 与 publish 一律 v2（删除 OFF 发 v1 三元门控）；`parse_consumer.py` 唯一 subject `ani.tasks.kb.parse.v2`；`.env.example` 删 `NATS_PARSE_SUBJECT`/`KB_PARSE_CONSUMER_ENABLED`；`test_parse_consumer.py` 新增 `test_config_parse_subject_is_v2_only` 断言无 `nats_parse_subject`/`kb_parse_consumer_enabled` 属性、`test_outbox_dispatcher.py` 删 Issue 037 rollback 用例；同步 kb-service proto 源 / generated / api 镜像注释。精确 grep（`ani\.tasks\.kb\.parse\b` 加 `\b` 边界避开 v2 前缀重叠）app/ 非 v2 引用清零；rag-engine 侧 `parse_worker`/`ani.tasks.kb.parse` 零匹配（#039 已清）。解析幂等键 `parse-{doc_id}-{run_id}-b{batch_no}` 语义不变。**验证：657 passed + 28 skipped + ruff E9,F63,F7,F82 All checks passed + mypy Success + validate-architecture 三脚本 + `git diff --check` exit 0**） |
| 9 | monorepo 构建/CI | 构建与门禁挂 monorepo 统一入口（Makefile/CI） | #062 KB-SPLIT-P3B（独立 CI + SemVer）；#068 KB-SPLIT-P5A（整目录迁出 + monorepo 收尾） | 已处置（#062，2026-10-08：kb-service 独立 CI/镜像流水线落地——`services/kb-service/Makefile` 自带门禁（`test` = lint + validate-architecture + validate-migrations + test-python；不依赖 monorepo 根 Makefile）+ `scripts/validate_kb_migrations.py`（migration baseline NNN 结构门禁）+ `scripts/validate_kb_sql_centralization.py`、`scripts/validate_kb_pg_init_consistency.py`（自包含副本）+ `deploy/init-scripts/kb-pg/00_kb_dedicated_roles.sql`（本地 init-script 副本）；独立 pipeline `.github/workflows/kb-service-ci.yml`（Path 过滤 `services/kb-service/**` 触发 lint/test/migration 门禁 job + tag `kb-v*.*.*`/dispatch 触发镜像构建 job，SemVer 格式强校验 + buildx 多架构 + Trivy 阻断 + Cosign 签名 + digest 证据），不再挂 monorepo 统一入口 `build-image.yml`（kb-service 不在任何 matrix）；镜像独立 SemVer tag（`harbor.ani.internal/ani/kb-service:MAJOR.MINOR.PATCH`，首个 v1.0.0），规则落 `docs/specs/semver-tagging.md`；`Dockerfile` 双栈 EXPOSE 8002 + 50053；`make test` 全绿（646 passed + 28 skipped）+ `git diff --check` 通过。注：monorepo 侧 Makefile kb 条目与 workflow 收尾归 #068 P5A） |
| 10 | 共享 async_tasks 幂等回放 | `grpc_server.py` 幂等回放写共享 async_tasks；共享 `pkg/repo/task_repo.go` 与 model-service 亦写入；Gateway task 路由读 async_tasks | #051 KB-SPLIT-P1B（kb_operations 幂等本地化）；#052 KB-SPLIT-P1C（ListOperations/GetOperation 临时进度查询）；#050 KB-SPLIT-P1A（kb 自有 outbox）；#069 KB-SPLIT-P5B（task 域/Gateway 最终对接） | outbox 部分已处置（#050，2026-09-21：kb 只写自有 `kb_outbox_events`，对 task 域解耦接口=事件 schema 稳定 + at-least-once 不变）；幂等回放已本地化（#051，2026-09-22：`migrations/010_kb_operations.sql` 建 `UNIQUE(tenant_id, idempotency_key)` 自有表，grpc_server 7 类 RPC 回放切换、删除 `app/repositories/async_task.py`，共享 async_tasks 不再被 kb 写入，进度对外只发 outbox 事件）；ListOperations 查询待 #052。部署时序确认（切换固有语义，#050 评审记录）：dispatcher 切自有表后，共享库 outbox_events 中遗留的 pending kb 事件不再被投递——拆分环境部署时按共享表 kb 事件已排空/接受滞留处理，消费者先订阅、dispatcher 后启动顺序不变，ADR 阶段 4 隔离验证覆盖 |

## 协作排期知会与回执（阶段 0 发出）

| # | 知会对象 | 内容 | 关联裁定 | 记录 | 回执状态 |
|---|---|---|---|---|---|
| ① | 前端（Console） | 文档上传从 Core 端点改为 kb 签发预签名 PUT 直传 + complete 登记；NotifyDocumentUploaded 不再存在 | D1 | [records/KB-SPLIT-P0-notice-frontend.md](records/KB-SPLIT-P0-notice-frontend.md) | 已发出 2026-09-21，待回复 |
| ② | Gateway | kb 资源路由改指向独立 kb-service（现状已是纯 gRPC 代理，切指向即可） | §1.6 | [records/KB-SPLIT-P0-notice-gateway.md](records/KB-SPLIT-P0-notice-gateway.md) | 已发出 2026-09-21，待回复 |
| ③ | task 拆分相关方 | kb outbox 事件 schema 稳定 + at-least-once 投递承诺；ListOperations/GetOperation 临时进度查询接口（task-service 就绪前） | D4 | [records/KB-SPLIT-P0-notice-task.md](records/KB-SPLIT-P0-notice-task.md) | 已发出 2026-09-21，待回复 |
| ④ | Core 团队 | Core 端点下线知会：kb 不再调用 vector-stores / knowledge-base-link / 预计算向量插入 / NotifyDocumentUploaded（kb 已自管 Milvus + MinIO 预签名直传 + complete） | D1/D3 | [records/KB-SPLIT-P4B-notice-core.md](records/KB-SPLIT-P4B-notice-core.md) | 已发出 2026-10-08，待回复 |

回执到达后在对应记录文件的「回执记录」表追加行，并同步更新本表状态。

## 当前阶段

| 阶段 | 内容 | 状态 |
|---|---|---|
| 0 | 基线固定 + 文档结构 + 差异清单 + 协作排期知会（本批次 KB-SPLIT-P0 / issue #049） | ✅ 完成（2026-09-21） |
| 1 | 数据与存储基线（#050-#059，关键路径；D1 存储基线并行） | 🔄 进行中（#050 KB-SPLIT-P1A kb 自有 outbox ✅ 2026-09-21；#051 KB-SPLIT-P1B kb_operations 幂等本地化 ✅ 2026-09-22；#052 KB-SPLIT-P1C ListOperations/GetOperation 进度查询 gRPC ✅ 2026-09-22；#053 KB-SPLIT-P1D B 层 4 表 DDL 基线 + 跨库 FK 移除 ✅ 2026-09-22；#054 KB-SPLIT-P1E RLS → 显式租户隔离 ✅ 2026-09-23（删 GUC/platform_bypass，9 repository 显式过滤，复合外键 `(tenant_id, id)` × 6，SQL 集中门禁，39 项跨租户负向测试）；#055 KB-SPLIT-P1F kb 专属 PG 实例 + 最小权限角色体系 + 真实 PG 跨租户测试 ✅ 2026-09-23（docker-compose.kb-pg.yml 127.0.0.1:5433/kb 专属 PG 17 与共享 5432/ani 并存、migrations/000 三角色全 NOSUPERUSER/NOBYPASSRLS、014 序列授权、003-007 零重放修复、config.py 默认指向专属库、真实 PG 28/28 跨租户负向、独立库全量 562 passed，证据三态 pass；#056 KB-SPLIT-P1G 向量自管：pymilvus 2.6.17 直连独立 Milvus + collection 生命周期 ✅ 2026-09-23（`app/vector_store/` AsyncMilvusClient 封装——显式 schema + AUTOINDEX COSINE、create/drop/recreate、insert/search/delete、懒连接单例；CreateKB 建 `kb_{kb_id}` + set_vector_store_id 落 collection 名（列语义平移 Core id → collection 名）、DeleteKB drop best-effort、DeleteDocument/reparse doc_id 过滤删、retrieve search 直连、rebuild recreate fail-fast；requirements pymilvus==2.6.17；21 项 Milvus 单测，555 passed + 28 skipped）；#057 KB-SPLIT-P1H 向量路径收口：写入-检索往返断言 + core_api vector 引用清零证据 ✅ 2026-09-24（纯收口批次生产代码零改动，6 项往返/重解析/删除测试 + core_api vector 引用 grep 清零证据，565 passed + 28 skipped）；#058 KB-SPLIT-P1I 对象自管：kb 自有 MinIO bucket 预签名 PUT 直传 + complete 端点 + 双视角 URL ✅ 2026-09-24（`app/object_store/` MinioObjectStore（minio==7.2.15 同步 SDK + asyncio.to_thread 桥接）——双视角双客户端（presign 走 `minio_public_endpoint` NodePort 对外、stat 走 `minio_endpoint` ClusterIP 对内、public_endpoint 空串 fallback 内网覆盖单节点/dev）、`presigned_put_object` 仅授 PUT 单操作 + TTL 配置化默认 900s、`stat_object` HeadObject 语义、NoSuchKey 二分（对象不存在 → ObjectNotFoundError/FAILED_PRECONDITION 拒绝登记 vs 网络故障 → ObjectStoreError/UNAVAILABLE 可重试）、懒构造进程单例；grpc_server presign 端点重写（成功路径零 DB 写，D1 三步流审计契约）+ complete 端点新增（HeadObject 校验 → 单事务四写 kb_documents pending → kb_operations task → kb_audit_log → kb_outbox parse；UNIQUE 幂等回放、ALREADY_EXISTS 重复拒绝、零审计拒绝路径）；proto CompleteDocumentUpload RPC + NotifyDocumentUploaded 标 DEPRECATED；config 8 个 MINIO_* + requirements minio==7.2.15；物理桶 `ani-kb-docs` 部署侧 compose 既有幂等行（kb 进程不自行建桶）；28 项新测试 + 3 既有文件断言迁移，593 passed + 28 skipped）；#059 对象生命周期与 Gateway 代理路由 KB-SPLIT-P1J ✅ 2026-09-28（新增 kb.v1 `GetObjectURL` RPC（object_id 语义 `{bucket}/{key}`，原地解析 + 双闸归属校验 + kb 存在性校验 + 内网 presign GET，FAILED_PRECONDITION→409 / NOT_FOUND→404 / UNAVAILABLE→503）；`MinioObjectStore` `remove_object` + `remove_objects_by_prefix` + `bucket`/`presign_ttl_seconds` property；`_delete_document` 正本 key 删 + 图片 `{kb_id}/{doc_id}/images/` 前缀删、`_delete_kb` `{kb_id}/` 全前缀删（best-effort，衔接 #053 CASCADE 语义）；`parse_orchestrator` 图片产物直连上传 kb bucket（key `{kb_id}/{doc_id}/images/{uuid}.{ext}`，markdown 占位 `[图片: caption](object_id)` 机制保留）；Gateway 纯 gRPC 代理 `CompleteDocumentUpload`/`GetObjectURL` + 两条路由（`POST .../documents/:doc_id/complete` 202、`GET .../objects?object_id=` 200，缺参 400/非本 KB 409/nil client 503），OpenAPI contract-first 同步 + 双端生成物重生成，route baseline 无需新增例外；`test_object_url.py` 10 项 + 删除清理断言 + object_store 6 项 + Gateway 两端点用例，615 passed + 28 skipped（P1I 基线 593 零回归）+ Gateway 全包 go test ok + route contract 0 error + validate-architecture + `git diff --check` 通过） |
| 2 | 契约固化（#060，可与阶段 1 并行） | ✅ 完成（2026-09-28，KB-SPLIT-P2：#060 proto 契约固化——kb-service `proto/` 单一真源 + 三对镜像 sha256 逐字节一致（pin），生成命令与版本锁定落 `docs/specs/proto-contract.md`，`tests/test_proto_contract.py` 22 项契约测试（pin 双侧一致 / 生成物↔proto 源一致性 / rag EmbedResponse 扁平数组 / 上传三步流时效-key-双视角 / D2 租户上下文 / RPC 面 28 全量），全量 639 passed + 28 skipped + validate-architecture + `git diff --check` 通过） |
| 3 | 构建/发布/运行独立（#061-#063，可与阶段 1 并行） | ✅ 完成（2026-10-08）（#061 KB-SPLIT-P3A 配置脱钩 ✅ 2026-10-08（`config.py` env_file 绝对路径锚定 kb 自有 `.env`、embedding 键 `KB_EMBEDDING_*` + AliasChoices 兼容旧键、新建 `.env.example` 全部配置项 + 敏感项占位符 + K8s Secret 注入说明、requirements `jieba==0.42.1` 全钉死，645 passed + 28 skipped + validate-architecture + `git diff --check` 通过；未新建 kb 专属 K8s 清单，随 P5 迁出落地）；#062 KB-SPLIT-P3B 独立 CI + SemVer ✅ 2026-10-08（`services/kb-service/Makefile` 自带门禁不依赖 monorepo 根：`test` = lint（ruff E9/F63/F7/F82 + mypy scripts）+ validate-architecture（SQL 集中化 + init/migration 一致性）+ validate-migrations（NNN baseline 结构）+ test-python；自包含校验脚本 `scripts/validate_kb_{migrations,sql_centralization,pg_init_consistency}.py` + `deploy/init-scripts/kb-pg/00_kb_dedicated_roles.sql`；独立 pipeline `.github/workflows/kb-service-ci.yml`（Path 过滤触发门禁 job、tag `kb-v*.*.*`/dispatch 触发镜像 job，SemVer 强校验 + buildx 多架构 + Trivy 阻断 + Cosign + digest 证据），kb 不在 monorepo `build-image.yml` 任何 matrix；镜像独立 SemVer tag `harbor.ani.internal/ani/kb-service:MAJOR.MINOR.PATCH`（首 v1.0.0），规则落 `docs/specs/semver-tagging.md`；`Dockerfile` EXPOSE 8002 + 50053；646 passed + 28 skipped + `git diff --check` 通过）；#063 KB-SPLIT-P3C 运行合同与可观测性 + 存储运维归属落地 ✅ 2026-10-08（`/readyz` 门控收敛为恰好三项 db + outbox_dispatcher + grpc，session_cache/consumers 只上报、NATS/rag-engine/Milvus/MinIO 走 `_probe_dependencies` TCP 探测上报项绝不门控；六类领域指标 `app/observability/metrics.py` 私有 `CollectorRegistry`（outbox 积压/consumer 在途重试/run 时长分布/三闸门命中/降级计数/presign-complete 成功率），标签只用有界枚举无高基数标签，41 处调用点接入，requirements 加 `prometheus-client==0.26.0`；日志关联四要素 `corr(request_id/kb_id/doc_id/run_id)` 结构化后缀不引入 logging 框架；运行合同保持（先订阅后 dispatcher/双 loop 资源绑定/JetStream ack 语义/优雅停机逆序）规格落 `docs/specs/observability.md`；Milvus 运维手册 `docs/runbook/storage-ops.md`（监控/备份/容量 + 现网 etcd 配额与 auto-compaction 教训继承：配额 8GiB、60% 阈值、CronJob `17 */6 * * *`、defrag `--command-timeout=300s`）；bucket `ani-kb-docs` 生命周期与配额 + presign 时效配置项化 `MINIO_PRESIGN_TTL_SECONDS` 默认 900s；`/metrics` 端点可被采集；修因子指标新增 `count_undispatched` 引入的 20 个 mock 回归——两 mock 连接类补 `fetchval`。验证：`make test` 全绿 646 passed + 28 skipped + validate-architecture + `git diff --check` 通过） |
| 4 | 隔离验证（#066/#067） | ✅ 完成（2026-10-09）（#064 KB-SPLIT-P4A v1 解析路径收敛删除 ✅ 2026-10-08——v1 subject + flag 门控 + OFF 发 v1 逻辑删除，v2 唯一路径消费者常开，657 passed + 28 skipped + validate-architecture + `git diff --check`；#065 KB-SPLIT-P4B Core 调用面清零 ✅ 2026-10-08——`app/core_api/` 整模块删除 + NotifyDocumentUploaded RPC 双端删除（RPC 面 28→27）+ 配置/契约/OpenAPI/Gateway/SDK 残留清零，对视 Core 仅剩租户上下文信任、出向 Core HTTP 调用清零，627 passed + 28 skipped + validate-architecture + route contract 0 error + `git diff --check`；**#066 KB-SPLIT-P4C 独立测试环境全链路验收 ✅ 2026-10-09（e2e verified，独立环境 + 真实依赖）**——专属 PG `10.10.1.66:30946`（9 表无 RLS）+ 独立 NATS stream `ani.tasks.kb.*`（解析 `ani.tasks.kb.parse.v2`）+ 独立 Milvus `10.10.1.66:31930` + kb 自管 bucket `ani-kb-docs` + 真实 rag-engine + 真实 embedding 推理服务 `qwen3-embedding-0.6b`（dim=1024 经 SSH 隧道接入）；产品链路级全链路（上传三步流 → 解析 v2 ready chunk=9 → 分块 child=5/parent=4 → 图片入库 GetObjectURL 取回 673B → 向量化 → 检索三模式 hybrid=4/vector=5/keyword=5 overlap=4 → 三闸门 → 会话 SSE + Redis/DB 双写）；可观测性（/metrics 200 + 六类指标真实数据 + 无高基数标签 `{consumer,gate,le,operation,outcome}`）；会话降级（Redis 挂→DB-only 5/5 PASS；NATS 挂→outbox 积压 published=FALSE → 恢复追平 published=TRUE + 文档 ready）；生产代码改动仅 `main.py` NATS 启动超时修复（`_NATS_STARTUP_TIMEOUT_SECONDS=5.0` + `asyncio.wait_for` 仅约束初始 connect、保留运行时重连）使降级实例可启动；rag-engine 零改动（未加 `chat_template_kwargs`）。**验证：e2e PASS 58 / FAIL 0 / NOT_VERIFIED 4（exit 0，证据层次 源码级 3/真实依赖级 32/产品链路级 27）+ `make test`（627 passed + 28 skipped）+ validate-architecture + validate-migrations + `git diff --check` exit 0；4 条 not_verified 诚实分级。测试后远程 kb-service replicas 已恢复 0→1（1/1 Running）**；**#067 KB-SPLIT-P4D 专项回归 ✅ 2026-10-09（e2e verified，独立环境 + 真实依赖）**——复用 #066 e2e 基建（importlib 加载 `test_issue066_e2e`，覆写日志/tag），新增 `tests/e2e/test_issue067_e2e.py` 98 项检查五类专项：① 降级路径（Redis 挂→会话 DB-only + `/readyz` status=ok 门控语义 + `/health` 200 + `kb_degradation_total{session_cache,redis_unavailable}=3.0`；NATS 挂→outbox 积压 `published=FALSE` + `/readyz` status=degraded（outbox_dispatcher 门控、nats healthy=False 仅上报）→ 恢复追平 `published=TRUE` + 文档 ready，at-least-once）；② 重解析幂等（garbage pdf 自然失败→reparse 可重跑且操作行闭环终态；md 文档两轮 reparse 各生成新 run_id（T1≠T2，run_id 载体=kb_operations 行 id）、新旧 chunk_id 零交集（9→9 overlap=0）、Milvus 向量数符合基线不变量（subset=child，vec 5→5 未叠加旧值）、kb.reparse 事件 payload.task_id==run_id）；③ 跨租户负向 16 项（另一租户 GetKB/UpdateKB（持有者幂等键，无跨租户回放泄漏）/DeleteKB/GetUploadURL/ListDocuments/GetDocument/DeleteDocument/Chunks/Permissions/Sessions/Messages/DeleteSession/Reparse/AuditLogs 全 404、ListKBs 不含宿主、gRPC ListOperations 空集 + GetOperation NOT_FOUND，owner 对照全通过、负向后 owner 资源完好）；④ D1 专项（篡改预签名 key 被拒且无对象落地；GetObjectURL 三档拒绝——无 `/`→400、bucket≠自有→409、前缀≠`{kb_id}/`→409；`MINIO_PRESIGN_TTL_SECONDS=2` 过期 PUT/GET 均 **403** 且预定 key 无对象落地；图片 `{kb}/{doc}/images/` 前缀批量删除 + 正本 `{kb}/{doc}`（无文件名后缀 key 规则）一并删除 + chunk 行/向量归零）；⑤ D4 专项（CreateKB 同名确定性键 `create_kb:{tenant}:{name}` 回放同 kb_id 且 kb.create 操作行恰 1、UpdateKB 同键回放结果一致且该键操作行恰 1、Reparse 同键 pending/completed 双态回放同 task_id 且终态回放先于 ready 门、副作用核对——操作行/事件/向量均无重复）。**验证：e2e PASS 98 / FAIL 0 / NOT_VERIFIED 0（exit 0，证据层次 真实依赖级 73/正式进程级 5/产品链路级 20）+ `make test`（627 passed + 28 skipped）+ validate-architecture + `git diff --check` exit 0。测试后远程 kb-service replicas 已恢复 0→1**。生产代码零改动（纯测试批次；测试基建 3 处自我修正落档：Windows taskkill 后端口释放竞态需 wait-ports-closed、asyncpg 对 JSONB 返回 str 需 json.loads、`/readyz` session_cache 为构造态上报（aioredis 惰性连接不 ping，非连通态）） |
| 5 | 仓库拆分 + 依赖方切换（#068/#069）+ 收尾（#070） | ✅ 完成（2026-10-10）（#068 KB-SPLIT-P5A ✅ 2026-10-09——整目录迁移独立仓库 ani-kb-service（拆分 commit `c5eb39e`，squash 快照策略落档）、不带入清单最终核对零残留（见「阶段 5 执行记录」）、rag-engine 无状态化核验、monorepo 侧收尾（目录删除 + CODEOWNERS + Makefile/ruff/go.work/allowlist/boundary/文档表述摘除 kb 条目 + CI workflow 随迁）、回滚策略登记、独立仓库自含门禁全绿；**#069 KB-SPLIT-P5B ✅ 2026-10-10——Gateway 与依赖方最终切换（见「阶段 5 执行记录（P5B）」）**：Gateway KB 路由指向独立 kb-service gRPC :50053（部署清单 + 注释落档、集群实态一致）、task 路由 D4 处置落档 + 对接知会、前端上传链路（D1）切换确认 + #049 知会闭环、P5A 移交契约面 2 error 清偿（v1.yaml security+x-ani-authz）与 proto pin 断言恢复（22 passed 0 skipped）、#066 环境复跑全链路 PASS 59 / FAIL 0 / NOT_VERIFIED 3 零回归 + 部署级证据三点；**#070 KB-SPLIT-P5C ✅ 2026-10-10——全量验收 + 文档闭环四件套 + ADR 0001 状态更新（见「阶段 5 收尾记录（P5C）」）**：阶段 0–5 验收证据汇总落档（含 not_verified 演进期豁免登记）、拆分目标五条核销、§七后果清单核销 + Milvus/bucket 运维责任交接确认、阶段 6 演进期事项登记、ADR 0001 状态 Proposed → Accepted，**拆分批次正式关闭**） |

## 阶段 5 执行记录（KB-SPLIT-P5A / issue #068，2026-10-09）

### 拆分仓库与历史策略

| 项 | 值 |
|---|---|
| 独立仓库 | `ani-kb-service`（自 ANI monorepo `repo/services/kb-service/` 整目录迁移，本文件随迁后继续作为拆分事务执行状态唯一真实来源） |
| 拆分 commit | `c5eb39e`（main root-commit，154 files / 55922 insertions） |
| 源快照 | monorepo 分支 `feat/split-kb-from-core` @ `4bc7ce09`（#065，分支末次提交）+ #066/#067 未提交验收态（tests/e2e 六个套件文件、main.py NATS 启动超时修复、本文件阶段 4 验收更新） |
| git 历史保留策略 | **squash 单快照**（ADR §五-阶段5 允许 filter-repo 或 squash 二选一，选型理由落档）：monorepo 分支上 #066/#067 验收产物（e2e 套件等 6 个文件 + main.py 修复）从未提交，filter-repo/subtree-split 只能重写已提交历史、无法承载未提交工作区，逐 commit 演进经下节基线链在 monorepo 侧追溯；首个 commit 即完整目标态，符合「首发即目标态」（ADR rev2 修订说明） |
| 随迁适配（迁移差异清单，其余整目录原样平移） | ① `.github/workflows/kb-service-ci.yml` 随迁并改仓库根上下文（`paths` 过滤移除、`KB_DIR` 删除、构建上下文 `context: .`/`file: Dockerfile`；SemVer tag 约定 `kb-v*.*.*` 与 buildx/Trivy/Cosign/digest 安全姿态不变）；② 新建 `.gitignore`（.venv/工具缓存/本地 .env/e2e 日志，monorepo 根 .gitignore 不随迁）；③ `tests/test_proto_contract.py` 跨仓库 proto pin 测试在镜像缺失（独立检出无 monorepo `api/proto/`、`ai/rag-engine/`）时 skip——镜像侧一致性归 monorepo 消费方（#069 依赖方切换），仓库内「生成物↔proto 源一致」12 项重生成比对测试继续强制；④ `.dockerignore` 头注释改指仓库根构建上下文 |

### 基线可追溯闭环（ADR §三-4）

`68bf198f`（#049 基线快照，2026-09-21）→ `feat/split-kb-from-core` 分支逐批提交（#050-#065）→ `4bc7ce09`（#065，分支末次提交）→ 工作区验收态（#066/#067，e2e 套件 + main.py 修复，未提交）→ **`c5eb39e`**（本仓库拆分 commit）。monorepo 侧保留分支与全部历史；本文件的差异清单各行（#050-#067 处置记录）即各阶段差异说明。

### 不带入清单最终核对（ADR §五-阶段5，git grep 逐项，证据面 = 拆分 commit 工作树 tracked 文件）

| 项 | 核对方式 | 结果 |
|---|---|---|
| v1 解析路径 | `git grep -E 'ani\.tasks\.kb\.parse\b'`（边界避开 v2 前缀重叠） | app/ 仅 2 处 docstring/注释历史说明（parse_consumer.py、config.py），零功能引用；发布/订阅面唯一 subject `ani.tasks.kb.parse.v2` |
| flag 门控 | `git grep 'kb_parse_consumer_enabled'` / `git grep 'nats_parse_subject'` | flag 仅存注释与 ADR/status 历史记载；`tests/test_parse_consumer.py:1257-1258` 断言 `nats_parse_subject`/`kb_parse_consumer_enabled` 属性不存在（#064 回归守卫）；live 键仅 `nats_parse_subject_v2` |
| NotifyDocumentUploaded | `git grep 'NotifyDocumentUploaded'` 全库 | app/proto/main.py/tests 零匹配（仅 docs/ ADR 与知会记录历史记载）；proto RPC 面 27 |
| core_api 模块 | `git grep -iE 'core_api\|CoreClient'`（app/main.py/tests） | 零匹配 |
| `app/generated` 散复制 | `git ls-files '*.proto'` + test_proto_contract 重生成比对 | 恰 4 个 .proto 全在 `proto/` 单一真源下（kb.v1/common.v1/rag.v1/inference.internal.v1），仓库内零散复制；`app/generated/*` 与 `app/rag_engine/rag_pb2*` 为 tracked 生成物，与源一致性由 12 项锁定版本重生成比对测试强制（通过） |
| 共享 async_tasks/outbox_events 读写 | SQL 形态 `git grep -iE '(into\|from\|update\|join)\s+(kb_)?(outbox_events\|async_tasks)\b'` | 实态 SQL 全部为 `kb_outbox_events`/`kb_operations`（kb 自有表，migrations 009/010）；裸 `outbox_events`/`async_tasks` 仅存解释性注释（含 #051 移除说明）；SQL 集中化门禁通过 |

### rag-engine 彻底无状态化核验（monorepo tracked 面，承接 #039/#064 验证记录）

`git grep -E 'parse_worker|ani\.tasks\.kb\.parse' -- repo/ai/rag-engine` → 零匹配；`git grep -iE 'asyncpg|pymilvus|psycopg|milvus' -- repo/ai/rag-engine/app` → 零匹配。rag-engine 无 parse 消费路径、无 PG/向量库直写依赖（无状态 gRPC 计算引擎）。

### monorepo 侧收尾清单（同日执行，删除与摘除明细）

- 删除 `repo/services/kb-service/` 整目录（含 .venv 本地工件）与 `repo/.github/workflows/kb-service-ci.yml`（随迁本仓库）。
- `.github/CODEOWNERS` 摘除 `/repo/services/kb-service/` 行（`/repo/api/proto/kb/` 与 Gateway `kb_resources*` 行保留至 #069）。
- `repo/Makefile`：`validate-architecture` 摘除 `validate_kb_sql_centralization.py`/`validate_kb_pg_init_consistency.py` 两行 + 删除 `validate-kb-pg-init-consistency` 目标与 .PHONY 条目（kb 侧自含副本随迁生效）。
- 删除 `repo/scripts/validate_kb_sql_centralization.py` 与 `repo/scripts/validate_kb_pg_init_consistency.py`（review-it 收口：校验对象已迁出、Makefile 引用已摘除后的死代码；kb 侧自含同名副本与门禁继续生效）。
- `repo/ruff.toml` 摘除 `services/kb-service/app/generated` 排除两行；`repo/go.work` 摘除 scaffold 注释 kb-service 行。
- `repo/architecture/component-import-allowlist.yaml` 摘除 kb-service asyncpg bounded_direct 条目块（11 项，#054/#055 期间设立）。
- `repo/scripts/validate_services_boundary.py` `SERVICES_OWNED_SOURCE_ROOTS` 摘除 `kb-service`（Gateway 禁导 kb-service Go 前缀 `FORBIDDEN_GATEWAY_SERVICE_IMPORT_PREFIXES` 保留——拆分后该边界仍成立）；`repo/scripts/validate_doc_entrypoints.py` 与 `ANI-05-系统架构设计.md` 同步摘除「kb-service 暂存 monorepo」表述。
- `CLAUDE.md` §3.3、`ANI-SERVICES-TEAM-GUIDE.md` 更新为「kb-service 已拆分独立仓库 ani-kb-service」。
- monorepo 保留项（依赖方切换归 #069 P5B）：`repo/api/proto/kb|common/v1/` Go proto 镜像、Gateway `kb_resources.go`/`kb_grpc_client.go`/`kb_sse.go` 路由、`repo/api/openapi/services/v1.yaml` 契约与 SDK、`repo/deploy/docker/`（docker-compose.kb-pg.yml + init-scripts/kb-pg，kb 专属 PG 本地部署载体）。

### 回滚策略登记（ADR §五-阶段5：git revert + 镜像重建，不设专门预案）

monorepo 侧回滚 = revert 本次收尾 commit（恢复目录与各门禁 kb 条目）；独立仓库侧回滚 = 仓库级回退（git revert / 重新 clone），镜像以最近可用 SemVer tag 重部署。无停写窗口/数据迁移/停用序列（rev2 无过渡性设计原则）。

### 本批次门禁（独立仓库自含，2026-10-09）

| 命令 | 结果 |
|---|---|
| `make test`（lint ruff E9/F63/F7/F82 + mypy scripts + validate-architecture（SQL 集中化 + PG init 一致性）+ validate-migrations（000..014）+ pytest） | ✅ 623 passed + 32 skipped（655 总数与 monorepo 阶段 4 基线一致；+4 skip = 跨仓库 proto pin 测试按随迁适配 skip） |
| `make validate-architecture` | ✅（含于 make test 链） |
| `git diff --check` | ✅ exit 0 |

## 阶段 5 执行记录（KB-SPLIT-P5B / issue #069，2026-10-10）

### Gateway 与依赖方最终切换（依赖方视角验收，monorepo 侧改动，记录：`repo/development-records/KB-SPLIT-P5B.md`）

| 项 | 处置 | 结果 |
|---|---|---|
| Gateway KB 路由指向（ADR §一-1.6） | monorepo 部署清单 `sprint13-production-shaped-gateway-deployment.yaml` 落档 `KB_SERVICE_GRPC_ADDR=kb-service.ani-system.svc.cluster.local:50053`（timeout 30s，与集群实态逐字一致）+ `kb_grpc_runtime.go` 注释更新（独立 kb-service :50053，纯 gRPC 代理不直连 kb 表，空地址 503 降级不变）；生产代码零行为变更 | ✅ |
| task 路由 D4 处置落档（ADR §D4/§五-阶段6） | Gateway `task_resources.go` 文件头处置注释（async_tasks 归 task 域资产、kb 自 #051 不再写共享表、演进期 task-service 就绪后接）+ 对接知会登记；不改 task 路由 | ✅ |
| 前端上传链路切换确认（ADR §D1） | 上传三步流（Gateway → kb 预签名 → 直传 MinIO NodePort → Gateway → kb complete）e2e 全程 PASS；与 #049 前端排期知会闭环（P1I/P1J 知会保持有效，本批为切换完成确认） | ✅ |
| P5A 移交契约面清偿 | monorepo `v1.yaml` completeDocumentUpload/getKnowledgeBaseObjectURL 补 `security`+`x-ani-authz`（v1/knowledge_base/create·read/tenant/[user]），`validate_services_contract.py` **2 error→0**（111 accepted warning 不变）+ 校验器单测 8 OK | ✅ |
| P5A 移交 proto pin 断言恢复 | `tests/test_proto_contract.py` 镜像根三级解析修复（原 parents[2] 拆分布局下恒指向 Desktop 致 4 项 pin 恒 skip）；修复后 **22 passed / 0 skipped**——kb.v1/common.v1/rag.v1/inference.internal.v1 四对镜像逐字节 pin 在「本仓库 + 并排 monorepo repo/」工作区内真实强制（与手工 sha256 比对双证一致）；真独立检出仍按 P5A 语义 skip | ✅ |
| #066 环境复跑全链路（ADR §五-阶段4） | e2e 路径适配拆分布局（KB_ROOT/REPO 分离：kb=本仓库、rag-engine/gateway exe=monorepo），gateway 以 `KB_SERVICE_GRPC_ADDR=localhost:50053` 驱动本仓库进程：**PASS 59 / FAIL 0 / NOT_VERIFIED 3**（exit 0，tag=e2e066-1791600485，源码级 3/真实依赖级 32/产品链路级 27；较 #066 基线 +1 PASS=闸门① 检索空自然触发，3 条 not_verified 与基线相同，零回归）；SSE `token*→sources→done` 经新指向端到端 PASS | ✅ |
| 部署级证据（real-k8s-lab/p5b_deploy_evidence_1010.sh） | ① gateway deployment env 指向 `kb-service.ani-system.svc.cluster.local:50053`；② kb-service Service endpoints `10.16.2.248:50053,10.16.2.248:8002`；③ 节点侧 ClusterIP:50053 HTTP/2 裸帧 gRPC Health/Check → http_code=200。e2e 期间集群 kb-service scale 0（NATS durable 竞争，components.md 既有约定）测后恢复 replicas=1（1/1 Running 已核实） | ✅ |
| CODEOWNERS 状态（P5A「保留至 #069」两行重审） | `/repo/api/proto/kb/`（Go 镜像消费方）与 Gateway `kb_resources*`（路由代理代码）确认保留——切换是地址级而非代码迁移，两行属 monorepo 侧消费资产，共同审查领地不变；注释更新为切换完成态 | ✅ |

### P5B 批次门禁（2026-10-10）

| 命令 | 结果 |
|---|---|
| 独立仓库 `pytest tests`（.venv） | ✅ **628 passed + 28 skipped**（pin 修复后终态：P5A 的 +4 skip 全部回归为真实断言，skip 数与拆分前基线一致，零回归） |
| `pytest tests/test_proto_contract.py -rs` | ✅ **22 passed（0 skipped）**——跨仓库 pin 断言自 P5A 平移后首次真实生效 |
| `ruff check --select E9,F63,F7,F82`（改动文件 e2e + proto_contract） | ✅ All checks passed（仓库门禁口径） |
| `git diff --check` | ✅ exit 0 |
| monorepo 侧门禁 | ✅ 等价直跑全绿（route contract 0 error / services contract 0 error / gateway-authz 327 路由 0 error / go test 51 包 ok，唯二 FAIL=pkg/adapters/runtime symlink ×2 为阶段 0 登记的 Windows 预存环境失败）——明细见 `repo/development-records/KB-SPLIT-P5B.md` |

## 阶段 5 收尾记录（KB-SPLIT-P5C / issue #070，2026-10-10）

> 拆分批次收尾：全量验收 + 文档闭环四件套 + ADR 0001 状态更新 + 演进期登记。**本批次为文档收尾性质，无业务代码改动**（ADR 0001 §五-阶段5、§三、§六、§七；依赖 #068/#069）。

### 全阶段验收证据汇总（AC1，ADR §五-阶段4/5）

> 每阶段证据按三态 `pass / fail / not_verified` 汇总；`not_verified` 项均附演进期豁免登记（见下小节），无未登记遗留。

| 阶段 | 批次 / issue | 验证层次 | 证据三态 | 关键证据 |
|---|---|---|---|---|
| 0 基线固定 | KB-SPLIT-P0 / #049 | 源码级 | **pass** | 基线快照 commit `68bf198f`；差异清单 10 项 × 处置 issue 落档；基线门禁全绿（validate-architecture + go test + `git diff --check`） |
| 1 数据与存储基线 | #050-#059 / P1A-P1J | 真实依赖级 | **pass** | kb 专属 PG 5433/kb 真实 28/28 跨租户负向全绿（#055）；向量自管 Milvus 往返（#057）、对象自管 bucket 预签名（#058/#059）；全量单测 615 passed + 28 skipped |
| 2 契约固化 | KB-SPLIT-P2 / #060 | 源码级 | **pass** | proto 单一真源 + 三对镜像 sha256 逐字节 pin；`test_proto_contract.py` 22 项契约测试通过；生成命令与版本锁定落 `docs/specs/proto-contract.md` |
| 3 构建/发布/运行独立 | #061-#063 / P3A-P3C | 源码级 | **pass** | 配置脱钩（`.env.example` + `KB_EMBEDDING_*`）；独立 CI + SemVer tag（`kb-v*.*.*`）；`/readyz` 三项门控 + 六类领域指标 + Milvus 运维手册就位 |
| 4 隔离验证 | #066/#067 / P4C-P4D | 产品链路级 | **pass** | 独立环境真实依赖全链路 PASS 59 / FAIL 0 / NOT_VERIFIED 3（#069 复跑，零回归）；专项回归 D1/D4/降级/幂等/跨租户 e2e PASS 98 / FAIL 0 / NOT_VERIFIED 0 |
| 5 仓库拆分 + 切换 + 收尾 | #068/#069/#070 / P5A-P5C | 部署级 | **pass** | 拆分 commit `c5eb39e`（154 files / 55922 insertions）；Gateway 指向独立 kb-service :50053（部署级证据三点）；独立仓库 pytest 628 passed + 28 skipped；双侧门禁全绿 |

### not_verified 演进期豁免登记（AC1）

| 项 | 来源 | 未验证原因 | 豁免 / 处置 |
|---|---|---|---|
| 摘要生成 content=null | #066 P4C AC | 推理模型对空/短输入返回 null，属模型侧行为非 kb 缺陷 | **演进期豁免**：不影响状态机与链路；如需收敛另立批次（不在拆分范围） |
| 闸门①「检索空」未自然触发 | #066 P4C AC | 真实语料检索恒非空，难以在真实环境构造 | **已消解**：#069 P5B 复跑时自然触发（PASS 59 较基线 +1），转 pass |
| 闸门③「safety net unreachable」 | #066 P4C AC | 防御性分支在真实链路不可达（去重后 sources 空为理论态） | **演进期豁免**：代码分支由单测覆盖，产品链路不可达属设计内；保留 not_verified 不虚报 |
| AC④-2 降级指标 = dispatcher 门控设计 | #066 P4C AC | NATS 挂时 `published=FALSE` 由 outbox_dispatcher 门控使实例 degraded，属设计内语义 | **演进期豁免**：已由 #067 专项回归（`/readyz` status=degraded 路径）真实覆盖，非缺陷 |

> 结论：阶段 0–5 无未登记 `not_verified` 遗留；4 条 not_verified 中 1 条已消解、3 条为设计内/模型侧行为并登记为演进期豁免。

### 拆分目标五条核销（AC3，ADR §三）

| # | 目标 | 核销 issue | 核销结论 |
|---|---|---|---|
| 1 | 独立维护（独立仓库/构建/版本/进程） | #062（独立 CI + SemVer）、#068（整目录迁出） | ✅ 独立仓库 `ani-kb-service`（首次 v1.0.0，`kb-v*.*.*` tag），独立 Makefile/CI 流水线/镜像，不挂 monorepo matrix |
| 2 | 独立持久化与存储 | #053（B 层 DDL 基线）-#058（对象自管） | ✅ A+B 层 + kb 自有 outbox（#050）+ kb_operations（#051）纳入单一 DDL 权威；独立 Milvus（#056/#057）+ 自有 bucket（#058/#059）；显式租户隔离不依赖 RLS（#054/#055） |
| 3 | 契约版本化且可演进 | #060（proto pin）、#065（Core 契约面归零） | ✅ 对上 proto + rag-engine proto 固定版本 pin（sha256 逐字节）；对 Core 无调用即无契约（`core_api` 删除、RPC 面 28→27） |
| 4 | 基线可追溯 | #049→#068 diff 链 | ✅ `68bf198f`（#049 基线）→ 分支 #050-#065 → `4bc7ce09`（#065）→ 工作区验收态 → `c5eb39e`（拆分 commit）；差异清单各行即各阶段差异说明 |
| 5 | 首发即目标态 | #064/#065 不带入清单核销 | ✅ v1 解析路径/flag 门控（#064）、NotifyDocumentUploaded/core_api/共享表读写（#065）以「代码不带入」消失；无迁移/停用/回滚预案，回滚 = git revert + 镜像重建 |

### ADR §七后果清单核销（AC6）

| 类型 | 条目 | 核销结论 |
|---|---|---|
| 正面红利 | kb 获得 A+B 层 + outbox + 幂等表单一 DDL 权威 | ✅ #053/#050/#051 落实 |
| 正面红利 | 向量与对象完全自管，热路径内网直连、少一跳 Core、URL 自主签发 | ✅ #056-#059 落实；#066 全链路真实依赖验证 |
| 正面红利 | 图片删除缺口从设计上不存在 | ✅ #059 前缀批量删除 + #067 D1 专项验证 |
| 正面红利 | Core 契约面为零，core_api 不带入 | ✅ #065 删除，grep 零匹配 |
| 正面红利 | 租户隔离从 RLS/GUC 转为显式约束 | ✅ #054 复合外键 + 显式过滤 + 跨租户负向测试 |
| 正面红利 | 三份 proto 复制收敛 | ✅ #060 单一真源 + 镜像 pin |
| 正面红利 | v1 不带入后 kb 唯一编排者，rag-engine 彻底无状态化 | ✅ #064/#039；rag-engine 侧 grep 零匹配 |
| 正面红利 | C 层争议消解（outbox 模式归位、task 资产留给 task 域） | ✅ #050 outbox 自持 + #051 幂等本地化 + #069 task 路由知会 |
| 正面红利 | 首发即目标态，无过渡性负担 | ✅ rev2 修订说明落实，无停写窗口/迁移/回滚预案 |
| 代价 | kb 承接 Milvus 与 bucket 运维责任 | ✅ **运维责任交接确认**：`docs/runbook/storage-ops.md`（#063 就位）——Milvus 监控/备份/容量 + etcd 配额与 auto-compaction 教训继承；bucket `ani-kb-docs` 生命周期/配额 + presign 时效配置化 |
| 代价 | 预签名直传需前端配合改造 + 时效/越权测试面 | ✅ #066/#067 D1 专项（过期 URL 403、key 越权 409、篡改 key 拒绝），#049 前端知会闭环（#069 确认切换完成） |
| 代价 | D2 暂缓意味着租户校验依赖 Gateway 信任链 | ✅ 原样保留，边界收敛 + 钩子留下（见演进期登记） |
| 明确不做 | 不换向量引擎 / 不动身份机制 / 不做通用文件服务 / 不设过渡性设计 | ✅ 全部遵守（冻结纪律，stage 0 落档） |

### 演进期（阶段 6）事项登记（AC5，ADR §五-阶段6）

> 本批次**不建 issue**，仅登记待后续规划排期。

| 事项 | 触发条件 | 备注 |
|---|---|---|
| pgvector 替代独立 Milvus 收敛评估 | 规模允许时 | 拆分期间遵守「平移优先」纪律未换引擎；评估消一套运维（ADR §D1、§1.5） |
| task-service 就绪后承接任务展示 | task-service 拆分完成 | kb 只承诺 outbox 事件 schema 稳定 + at-least-once；Gateway task 路由届时接 task-service（D4 解耦接口；#069 已落注释知会） |
| D2 身份机制调整 | 服务间认证换型需求出现 | 只改边界层与租户校验实现，不动业务逻辑；届时另立 ADR |
| 未来 Core 依赖 RFC 流程 | 出现新 Core 依赖 | kb 提案（端点/动机/兼容性）→ Core 评审 → 发新版本 → pin 升级；不允许静默变更（ADR §D3） |

### ADR 0001 状态更新（AC4）

- 执行侧（唯一权威）`docs/adr/0001-split-kb-from-core.md` 与规划侧（历史稿）`repo/services/tasks/modules/plan/knowledge_base/0001-split-kb-from-core.md` 状态行 **Proposed → Accepted**。
- 附引用：拆分 commit `c5eb39e`；验收证据 = 本文件阶段 0–5 证据汇总 + `repo/development-records/KB-SPLIT-P5C.md`。

### P5C 批次门禁（2026-10-10，双侧）

| 命令 | 结果 |
|---|---|
| 独立仓库 `make test`（lint + validate-architecture + validate-migrations + pytest） | ✅ **628 passed + 28 skipped** + ruff All checks passed + mypy Success + SQL 集中化/init 一致性/migration baseline 全绿 |
| 独立仓库 `git diff --check` | ✅ exit 0 |
| monorepo `make validate-services-route-contract` | ✅ 9 accepted / **0 error** + 校验器单测 7 OK |
| monorepo `git diff --check` | ✅ exit 0 |

### 拆分批次关闭

KB-SPLIT 全系 #049–#070 全部关闭；拆分工程（阶段 0–5）完成，阶段 6 演进期事项已登记。**kb-service 拆分批次正式关闭。**
