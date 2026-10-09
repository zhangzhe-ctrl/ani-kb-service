"""D1 对象自管：minio SDK 直连 kb 自有 bucket（issue-058, ADR §D1）。

kb 不再经 Core objects 中转登记/预签名，新建自有 bucket（默认
``ani-kb-docs``，e2e compose 先例名），key 规则：

    正本   {kb_id}/{doc_id}
    产物   {kb_id}/{doc_id}/images/{uuid}.{ext}   （#059 落地）

上传三步流（ADR §D1）：
    1. kb 签发预签名 PUT（本模块 ``presigned_put_object``，走
       publicEndpoint/NodePort 对外视角——前端浏览器可达）
    2. 前端直传 MinIO（不经过 kb/Core）
    3. complete 端点 HeadObject 校验（本模块 ``stat_object``，走
       internal/ClusterIP 视角——集群内取用）后登记 pending 文档

解析取用（ADR §D1 line 138）：解析时 kb 签发**内网视角**预签名 GET
URL（``presigned_get_object(view="internal")``，ClusterIP）给
rag-engine；**前端**取用图片产物走 ``view="public"``（NodePort，浏览器
集群外可达）；图片产物由 kb 直接 ``put_object`` 进同一 bucket（不再经
Core objects 中转）。

双视角 endpoint（对齐 Core Go ``MinIOObjectStore`` 先例
``pkg/adapters/objectstore/minio_store.go``）：
    presign PUT          → public_endpoint（对外 NodePort），空则 fallback 内网
    presign GET public   → public_endpoint（对外 NodePort），前端浏览器取用
    presign GET internal → endpoint（内网 ClusterIP），rag-engine 集群内取用
    stat / put           → endpoint（内网 ClusterIP），kb 进程在集群内走最短路径

固定 region（``us-east-1``，Core Go 先例 ``defaultMinIORegion``）：
minio SDK 的 ``presigned_put_object`` / ``presigned_get_object`` 在
构造器未传 region 时会先发 GetBucketLocation 网络请求——固定 region
后 presign 退化为纯本地字符串拼接（SigV4），签发路径零额外 IO。

minio SDK 为同步实现（urllib3 连接池，线程安全）：本模块以
``asyncio.to_thread`` 包装调用，gRPC handler（_grpc_loop 协程）与
uvicorn loop（consumer）都可直接 await，不阻塞事件循环。

懒导入 SDK：进程启动不依赖 MinIO 可达；``client``/``public_client``
注入点供单测 fake（需实现 presigned_put_object / presigned_get_object /
stat_object / put_object 方法）。
"""
from __future__ import annotations

import asyncio
import io
import logging
from datetime import timedelta
from typing import Any
from urllib.parse import urlsplit

from app.core.config import settings

logger = logging.getLogger(__name__)


class ObjectStoreError(Exception):
    """直连 MinIO 操作失败（minio SDK 异常的统一包装）。"""


class ObjectNotFoundError(ObjectStoreError):
    """HeadObject 校验失败：对象不存在（NoSuchKey / 404）。

    与 MinIO 网络不可达区分开——complete 端点把本错误映射为业务
    拒绝 FAILED_PRECONDITION（对象未上传就登记是客户端流程违约），
    其余 ObjectStoreError 映射 UNAVAILABLE（基础设施故障）。
    """


def normalize_minio_endpoint(endpoint: str) -> str:
    """MINIO_ENDPOINT / MINIO_PUBLIC_ENDPOINT → minio SDK endpoint。

    沿用 .env 已有键（Core Go 同键 OBJECT_STORE_*, 语义一致）：形如
    ``localhost:9000``（内网）或 ``10.10.1.66:31950``（NodePort 对外）。
    minio SDK 的 endpoint 参数要求不带 scheme（secure 布尔单独传），
    允许传入带 http/https scheme 的值时剥掉 scheme。
    """
    endpoint = endpoint.strip()
    if "://" in endpoint:
        parts = urlsplit(endpoint)
        return parts.netloc or endpoint
    return endpoint


class MinioObjectStore:
    """kb 自有 MinIO bucket 的异步客户端封装（同步 SDK + to_thread 桥接）。

    生产默认懒连接：首次操作时按 settings.minio_* 创建 minio SDK
    ``Minio`` 客户端（失败在首个操作上抛 ObjectStoreError）。两个
    endpoint 各一个客户端：
        ``_internal_client``：stat_object / HeadObject（集群内取用）
        ``_public_client``：presigned_put_object（对外视角签发）
    ``client`` 注入内部视角、``public_client`` 注入对外视角，单测 fake
    可只实现用到的方法子集。
    """

    def __init__(
        self,
        *,
        endpoint: str | None = None,
        public_endpoint: str | None = None,
        access_key: str | None = None,
        secret_key: str | None = None,
        bucket: str | None = None,
        region: str | None = None,
        presign_ttl_seconds: int | None = None,
        secure: bool | None = None,
        client: Any = None,
        public_client: Any = None,
    ) -> None:
        self._endpoint = endpoint if endpoint is not None else settings.minio_endpoint
        # public endpoint: explicit arg or settings; empty string (either
        # source) means "single-node / dev — both views coincide" and
        # falls back to the internal endpoint (ADR §一-1.6).
        explicit_public = (
            public_endpoint
            if public_endpoint is not None
            else settings.minio_public_endpoint
        )
        self._public_endpoint = explicit_public or self._endpoint
        self._access_key = access_key if access_key is not None else settings.minio_access_key
        self._secret_key = secret_key if secret_key is not None else settings.minio_secret_key
        self._bucket = bucket if bucket is not None else settings.minio_bucket
        self._region = region if region is not None else settings.minio_region
        self._presign_ttl = (
            presign_ttl_seconds
            if presign_ttl_seconds is not None
            else settings.minio_presign_ttl_seconds
        )
        self._secure = secure if secure is not None else settings.minio_secure
        self._client = client          # 注入：内部视角 fake（stat）
        self._public_client = public_client  # 注入：对外视角 fake（presign）
        self._internal_built: Any = None
        self._public_built: Any = None

    # ── 客户端构造（懒；region 固定使 presign 纯本地计算）────────────────

    @property
    def bucket(self) -> str:
        """kb 自有 bucket 名（解析产物 markdown 占位 URL 的 ``bucket/key`` 前缀）。"""
        return self._bucket

    @property
    def presign_ttl_seconds(self) -> int:
        """预签名 URL 默认有效期（秒），随签发结果一并回传给调用方。"""
        return self._presign_ttl

    def _build_client(self, endpoint: str) -> Any:
        from minio import Minio

        return Minio(
            normalize_minio_endpoint(endpoint),
            access_key=self._access_key or None,
            secret_key=self._secret_key or None,
            secure=self._secure,
            region=self._region,
        )

    def _get_internal_client(self) -> Any:
        if self._client is not None:
            return self._client
        if self._internal_built is None:
            self._internal_built = self._build_client(self._endpoint)
        return self._internal_built

    def _get_public_client(self) -> Any:
        if self._public_client is not None:
            return self._public_client
        if self._public_built is None:
            self._public_built = self._build_client(self._public_endpoint)
        return self._public_built

    async def aclose(self) -> None:
        """进程退出路径钩子。minio SDK（urllib3 池）无显式 close，no-op。"""
        return None

    # ── 上传三步流（presign 签发 / complete 校验）────────────────────────

    async def presigned_put_object(self, *, key: str, expires_seconds: int | None = None) -> str:
        """签发预签名 PUT URL（对外/NodePort 视角，前端直传用）。

        key 规则见模块 docstring（正本 ``{kb_id}/{doc_id}``）。仅授权
        PUT 单一动词（minio presigned_put_object 固定 PUT，无 GET/List
        泄露面）。expires 上限 7 天（SDK 校验），kb 默认 900s
        （MINIO_PRESIGN_TTL_SECONDS，Core Go 先例 15 分钟）。

        region 已在构造器固定，SDK 退化为纯本地 SigV4 字符串拼接
        （无 GetBucketLocation 网络请求），故直接调用（不经 to_thread）：
        避免为一段无阻塞的 CPU 计算占用默认线程池（与 stat 的
        to_thread 共享池，避免互相挤占）。
        """
        ttl = expires_seconds if expires_seconds is not None else self._presign_ttl
        if ttl <= 0:
            raise ObjectStoreError(f"invalid presign TTL: {ttl!r}")
        client = self._get_public_client()
        try:
            url = client.presigned_put_object(
                self._bucket, key, timedelta(seconds=ttl)
            )
        except Exception as exc:
            raise ObjectStoreError(f"presign PUT {self._bucket}/{key} failed: {exc}") from exc
        return str(url)

    async def presigned_get_object(
        self,
        *,
        key: str,
        expires_seconds: int | None = None,
        view: str = "internal",
    ) -> str:
        """签发预签名 GET URL（视角随消费方而变）。

        - ``view="internal"``（默认，ClusterIP）：**解析时** kb 签发内网
          视角 URL 给 rag-engine 下载文档正本——集群内取用走最短路径，
          不暴露对外 NodePort（ADR §D1 line 138）。
        - ``view="public"``（NodePort）：**前端**取用图片产物——URL 由
          浏览器直连 MinIO 打开，必须签发对外可达地址（与
          ``presigned_put_object`` 同视角；对齐 Core Go
          ``SignedDownloadURL`` 先例：内网地址链接只在集群内可解析，
          集群外 DNS 失败）。

        key 规则同 PUT（正本 ``{kb_id}/{doc_id}``、产物
        ``{kb_id}/{doc_id}/images/{uuid}.{ext}``），仅授权 GET 单一动词。
        region 固定 → 纯本地 SigV4，直接调用不经 to_thread。
        """
        if view not in ("internal", "public"):
            raise ObjectStoreError(f"invalid presign view: {view!r}")
        ttl = expires_seconds if expires_seconds is not None else self._presign_ttl
        if ttl <= 0:
            raise ObjectStoreError(f"invalid presign TTL: {ttl!r}")
        client = (
            self._get_public_client()
            if view == "public"
            else self._get_internal_client()
        )
        try:
            url = client.presigned_get_object(
                self._bucket, key, timedelta(seconds=ttl)
            )
        except Exception as exc:
            raise ObjectStoreError(f"presign GET {self._bucket}/{key} failed: {exc}") from exc
        return str(url)

    async def put_object(
        self,
        *,
        key: str,
        content: bytes,
        content_type: str | None = None,
    ) -> None:
        """直传对象进 kb 自有 bucket（内网视角，解析产物图片用）。

        ADR §D1：解析产物图片由 kb 唯一读写，key 规则
        ``{kb_id}/{doc_id}/images/{uuid}.{ext}``。minio SDK ``put_object``
        为同步网络 IO，用 to_thread 桥接避免阻塞事件循环。
        """
        client = self._get_internal_client()

        try:
            await asyncio.to_thread(
                client.put_object,
                self._bucket,
                key,
                io.BytesIO(content),
                len(content),
                content_type=content_type or "application/octet-stream",
            )
        except Exception as exc:
            raise ObjectStoreError(f"put {self._bucket}/{key} failed: {exc}") from exc

    async def stat_object(self, *, key: str) -> dict[str, Any]:
        """HeadObject 校验（内网视角）：对象不存在时抛 ObjectNotFoundError。

        返回 {size, etag, content_type, last_modified}（complete 端点
        校验对象真实存在并登记 file_size_bytes 用）。minio SDK 对
        不存在的对象抛 ``minio.error.S3Error``（code="NoSuchKey"）或
        urllib3 MaxRetryError 网络错误——前者包装为
        ObjectNotFoundError（业务拒绝），后者保持 ObjectStoreError
        （基础设施故障）。
        """
        client = self._get_internal_client()
        try:
            stat = await asyncio.to_thread(client.stat_object, self._bucket, key)
        except Exception as exc:
            if _is_no_such_key(exc):
                raise ObjectNotFoundError(
                    f"stat {self._bucket}/{key} failed: object not found"
                ) from exc
            raise ObjectStoreError(f"stat {self._bucket}/{key} failed: {exc}") from exc
        return {
            "size": stat.size,
            "etag": stat.etag,
            "content_type": stat.content_type,
            "last_modified": stat.last_modified,
        }

    # ── 生命周期：删除清理（ADR §D1-删除）─────────────────────────────

    async def remove_object(self, *, key: str) -> bool:
        """按 key 删单个对象（内网视角）。不存在视为成功（幂等）。

        文档删除时清理正本 ``{kb_id}/{doc_id}``（KB-SPLIT-P1J #059）。
        minio SDK ``remove_object`` 对不存在的对象不报错（S3 DELETE 幂等），
        故本方法总是返回 True；真正的故障（网络/权限）抛 ObjectStoreError。
        """
        client = self._get_internal_client()
        try:
            await asyncio.to_thread(client.remove_object, self._bucket, key)
        except Exception as exc:
            raise ObjectStoreError(f"remove {self._bucket}/{key} failed: {exc}") from exc
        return True

    async def remove_objects_by_prefix(self, *, prefix: str) -> int:
        """按前缀批量删对象（内网视角），返回删除计数。

        文档删除清理图片产物 ``{kb_id}/{doc_id}/images/``；KB 删除清理
        全前缀 ``{kb_id}/``（KB-SPLIT-P1J #059，ADR §D1-删除）。先
        ``list_objects``（recursive）枚举再逐个 ``remove_object``——kb
        对象量级小（图片按文档聚合），无需分批 DeleteObjects 批量协议。
        单个对象删除失败使其抛 ObjectStoreError（调用方 best-effort 兜底）。
        """
        client = self._get_internal_client()
        try:
            names = await asyncio.to_thread(self._list_object_names, client, prefix)
        except Exception as exc:
            raise ObjectStoreError(f"list {self._bucket}/{prefix} failed: {exc}") from exc
        for name in names:
            try:
                await asyncio.to_thread(client.remove_object, self._bucket, name)
            except Exception as exc:
                raise ObjectStoreError(
                    f"remove {self._bucket}/{name} failed: {exc}"
                ) from exc
        return len(names)

    def _list_object_names(self, client: Any, prefix: str) -> list[str]:
        """枚举 prefix 下所有对象名（同步 SDK 迭代器；供 to_thread 调用）。"""
        return [
            obj.object_name
            for obj in client.list_objects(
                self._bucket, prefix=prefix, recursive=True
            )
        ]


def _is_no_such_key(exc: Exception) -> bool:
    """minio SDK NoSuchKey 识别（懒导入，进程启动不依赖 minio 可达）。"""
    from minio.error import S3Error

    return isinstance(exc, S3Error) and getattr(exc, "code", None) == "NoSuchKey"


# 进程级单例（懒构造，构造无 IO；多租户共享 bucket，租户隔离由
# key 前缀 {kb_id}/ 承载）。同步 SDK 无 loop 绑定问题，单例可被
# kb 进程的两个事件循环（uvicorn / _grpc_loop）直接共享。
_default_object_store: MinioObjectStore | None = None


def get_default_object_store() -> MinioObjectStore:
    """Return the process-wide MinioObjectStore singleton (lazy connect)."""
    global _default_object_store
    if _default_object_store is None:
        _default_object_store = MinioObjectStore()
    return _default_object_store
