"""kb-service configuration (SPEC §2.4).

KB-SPLIT-P3A (issue-061, ADR 0001 §四 #5 / §五-阶段3): configuration is
decoupled from the monorepo shared root `.env`. The kb-service loads its
own `.env` (repository root of this service, see `.env.example`), so the
service no longer reads the shared repo-root file. Environment names are
case-insensitive (pydantic-settings): these fields map to DATABASE_URL /
NATS_URL / REDIS_URL, or to the kb-dedicated
KB_EMBEDDING_MODEL / KB_EMBEDDING_DIM (falling back to the legacy shared
EMBEDDING_MODEL / EMBEDDING_DIM). Extra env vars are ignored
(``extra="ignore"``), so a shared environment remains safe.
"""
from pathlib import Path

from pydantic import AliasChoices, Field
from pydantic_settings import BaseSettings, SettingsConfigDict

# kb-service 自有 .env（本服务仓库根，随 KB-SPLIT-P5 整目录迁出——ADR
# §五-阶段3-配置脱钩）。以绝对路径锚定，不再依赖进程 CWD 命中 monorepo
# 根 `.env`；文件不存在时 pydantic-settings 静默跳过（仅走 os.environ）。
_KB_ENV_FILE = Path(__file__).resolve().parents[2] / ".env"


class Settings(BaseSettings):
    # gRPC server
    grpc_port: int = 50053

    # PostgreSQL (asyncpg) — maps to env DATABASE_URL.
    # KB-SPLIT-P1F (issue-055, ADR 0001 §四 #1): kb-service 默认指向 kb
    # 专属 PostgreSQL 实例（deploy/docker/docker-compose.kb-pg.yml：
    # 127.0.0.1:5433/ani-kb-postgres），以最小权限应用角色 ani_app_user
    # 连接（migrations/000_kb_dedicated_roles.sql），不再默认共享库
    # localhost:5432/ani。生产/远程环境经 DATABASE_URL env 覆盖。
    database_url: str = "postgresql://ani_app_user:ani_app_dev_password@localhost:5433/kb"

    # rag-engine gRPC (Parse/Embed/Generate). Plan §3.4: the stateless
    # RPCs are accessed via gRPC (default localhost:50052).
    rag_engine_grpc_addr: str = "localhost:50052"

    # KB-SPLIT-P1G (issue-056, ADR 0001 §D1): kb 直连独立 Milvus 实例
    # （pymilvus 进 kb 进程，不经 Core vector-stores 中转）。
    # MILVUS_ADDR 与 rag-engine 同键。credentials 归 KB-SPLIT-P3A（#061）：
    # milvus_token 由 K8s Secret 注入（见 .env.example 占位符），不在
    # 提交物中落真实凭据；MILVUS_TOKEN 空 = 无鉴权连接（独立实例 dev 默认）。
    milvus_addr: str = "localhost:19530"
    milvus_token: str = ""
    # 客户端操作默认超时（秒）。D1 直连后 kb 与 Milvus 之间无中间层超时兜底，
    # 不传 timeout pymilvus 会无限等待（Milvus/etcd 挂起时拖死 Query/parse）。
    milvus_timeout: float = 10.0

    # KB-SPLIT-P1I (issue-058, ADR 0001 §D1): kb 自有 MinIO bucket 预签名
    # 直传。kb 不再经 Core objects 中转，正本 key `{kb_id}/{doc_id}`。
    # MINIO_* 键沿用共享 .env 语义（Core Go 同语义键为 OBJECT_STORE_*
    # 前缀）。access/secret 归 KB-SPLIT-P3A（#061）：由 K8s Secret 注入
    # （见 .env.example 占位符），提交物中不落真实凭据。
    minio_endpoint: str = "localhost:9000"          # 内网/ClusterIP 视角（stat）
    minio_public_endpoint: str = ""                # NodePort 对外视角（presign）；空 = fallback 内网
    minio_access_key: str = ""
    minio_secret_key: str = ""
    # minio_secure 的 MINIO_USE_SSL 为共享键，MINIO_SECURE 是 e2e 测试
    # 既有键——AliasChoices 两者都认（Core Go 同语义先例 OBJECT_STORE_USE_SSL）。
    minio_secure: bool = Field(default=False, validation_alias=AliasChoices("MINIO_SECURE", "MINIO_USE_SSL"))
    # kb 自有 bucket（e2e compose 先例名）；region 固定使 presign 纯本地
    # 计算（Core Go defaultMinIORegion 先例）；presign TTL 默认 15 分钟
    # （Core Go 先例，SDK 上限 7 天）。
    minio_bucket: str = "ani-kb-docs"
    minio_region: str = "us-east-1"
    minio_presign_ttl_seconds: int = 900

    # inference-service gRPC (internal control API). Used to resolve a
    # published served_model_name to its cluster runtime endpoint via
    # ResolveInferenceServiceEndpoint; the endpoint is then passed through
    # to rag-engine so Embed/Generate connect directly to the tenant's
    # own inference service (no Gateway routing by API key).
    inference_service_grpc_addr: str = (
        "inference-service.ani-system.svc.cluster.local:9104"
    )

    # NATS (outbox dispatch) — maps to env NATS_URL.
    nats_url: str = "nats://localhost:4222"
    # Parse subject, consumed by the kb-service NATS consumer
    # (app/consumers/parse_consumer.py). KB-SPLIT-P4A (ADR 0001 §一-1.3):
    # this is the sole parse path — the legacy ``ani.tasks.kb.parse`` v1
    # subject and the ``kb_parse_consumer_enabled`` gate are not carried
    # over. The consumer is always on; the Outbox Dispatcher always
    # publishes here.
    nats_parse_subject_v2: str = "ani.tasks.kb.parse.v2"

    # P1 #24 full-KB rebuild: dedicated subject for kb.rebuild outbox
    # events (OutboxDispatcher routes event_type 'kb.rebuild' here via
    # subject_overrides), consumed by app/consumers/rebuild_consumer.py.
    # Distinct from the parse subjects: rebuild is a long serial job and
    # must not interleave with per-document parse traffic.
    nats_rebuild_subject: str = "ani.tasks.kb.rebuild.v1"

    # P1 #24: kb-service rebuild consumer flag (default OFF). When False
    # the consumer does not start; rebuild outbox events stay queued until
    # the flag is enabled (at-least-once via the outbox, no loss).
    kb_rebuild_consumer_enabled: bool = False

    # Redis (session cache) — maps to env REDIS_URL
    redis_url: str = "redis://localhost:6379/0"

    # Embedding model — KB-SPLIT-P3A (issue-061, ADR 0001 §一-1.4 /
    # §五-阶段3): renamed to kb-dedicated keys KB_EMBEDDING_MODEL /
    # KB_EMBEDDING_DIM and declares the source explicitly (kb's own .env).
    # AliasChoices keeps the legacy shared EMBEDDING_MODEL / EMBEDDING_DIM
    # readable (same AliasChoices precedent as minio_secure below), so an
    # existing shared environment still works while kb migrates to its own
    # keys.
    #
    # embedding_model 无默认向量模型：默认空串表示"未配置"（不 fail-fast，
    # 服务可启动）。CreateKB 优先用请求体显式值，缺省时回落本值；两者都
    # 为空则返回 INVALID_ARGUMENT（见 grpc_server.create_kb）。SiliconFlow
    # 端点只认全名前缀（如 "BAAI/bge-m3"；裸名 "bge-m3" 会 model_not_found），
    # 配置时需填完整名。
    embedding_model: str = Field(
        default="",
        validation_alias=AliasChoices("KB_EMBEDDING_MODEL", "EMBEDDING_MODEL"),
    )
    # embedding_dim 是"探测失败回退值"，不是维度真源：CreateKB/rebuild 用
    # 选定模型实测一条短文本取返回 dimension 自适应 collection 维度，仅当
    # 探测失败（网络异常/模型名无效等）才回落本值。
    embedding_dim: int = Field(
        default=1024,
        validation_alias=AliasChoices("KB_EMBEDDING_DIM", "EMBEDDING_DIM"),
    )

    model_config = SettingsConfigDict(
        # kb 自有 .env（非 monorepo 根）；绝对路径锚定，CWD 无关。
        env_file=_KB_ENV_FILE,
        env_file_encoding="utf-8",
        extra="ignore",
    )


settings = Settings()
