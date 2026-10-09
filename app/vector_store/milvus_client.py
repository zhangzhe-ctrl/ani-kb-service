"""D1 向量自管：pymilvus AsyncMilvusClient 直连独立 Milvus（issue-056, ADR §D1）。

kb 进程内直连 insert/search/delete，不经 Core vector-stores 登记/中转。
collection 命名沿用 `vector_store_name = "kb_{kb_id}"` 派生（grpc_server
的 ``_vector_store_name``），CreateKB 建 / DeleteKB 删 / rebuild 重建，
无存量迁移。

Schema（对齐 Core 托管 Milvus 时代的既有形态，平移不换型）：
    id       VARCHAR  primary key（chunk_id, uuid4）
    vector   FLOAT_VECTOR(dim)   — dim 与 KB 的 embedding 模型实测对齐
    content  VARCHAR             — chunk 文本（检索免二次 PG 回查）
    metadata JSON                — chunk_id/chunk_type/parent_content/
                                   parent_chunk_id/doc_id/file_name/
                                   page_number/content_type
索引：AUTOINDEX + COSINE（与 Core API 的 metric="cosine" 语义一致，
Milvus COSINE distance 即余弦相似度，越大越相关）。

search 输出保持 Core `searchVectorStore` 的 items 形状
（{id, score, content, metadata}），retrieve_service / query 路径零改动；
insert 输入保持 Core `insertVectorStoreDocuments` 的 documents 形状
（{id, content, vector, metadata}），parse_orchestrator 只换调用点。

跨 loop 隔离：kb 进程是双 loop 结构（uvicorn loop 跑 parse/rebuild
consumer，专用 _grpc_loop 跑 gRPC handler），``AsyncMilvusClient`` 底层
grpc.aio channel 绑定首次使用它的 loop——单例 store 被两个 loop 共享时，
client 按操作时的 running loop 分池（一个 loop 一个 client），调用方零感知
（真实 Milvus 冒烟确认的缺陷修复，详见 development-records/KB-SPLIT-P1G.md）。
"""
from __future__ import annotations

import asyncio
import logging
from typing import Any
from weakref import WeakKeyDictionary

from app.core.config import settings

logger = logging.getLogger(__name__)

# Schema 常量。id 为 uuid4 hex/带连字符字符串（64 足够）；content 上限取
# Milvus VARCHAR 最大值（parent_content 以 metadata JSON 承载，不受此限）。
_PK_MAX_LENGTH = 64
_CONTENT_MAX_LENGTH = 65535


class VectorStoreError(Exception):
    """直连 Milvus 操作失败（pymilvus 异常的统一包装）。"""


def normalize_milvus_uri(addr: str) -> str:
    """MILVUS_ADDR → AsyncMilvusClient uri。

    MILVUS_ADDR 沿用 .env 已有键（rag-engine 同键，如 ``localhost:19530``/
    ``10.10.1.66:31950``），无 scheme 时补 ``http://``。
    """
    addr = addr.strip()
    if "://" not in addr:
        return f"http://{addr}"
    return addr


class MilvusVectorStore:
    """独立 Milvus 实例的异步客户端封装。

    生产默认懒连接：首次操作时按 settings.milvus_addr / settings.milvus_token
    创建 ``AsyncMilvusClient``（连接失败在首个操作上抛 VectorStoreError）。
    ``client`` 注入点供单测使用（fake 需实现 create_collection /
    has_collection / drop_collection / insert / search / delete 方法子集）。

    事件循环隔离：``AsyncMilvusClient`` 底层 grpc.aio channel 绑定首次使用
    它的事件循环。kb 进程双 loop（uvicorn loop 跑 parse/rebuild consumer，
    专用 _grpc_loop 跑 gRPC handler）会共享同一个 store 实例——生产路径的
    client 按操作时的 running loop 分池（一个 loop 一个 client），实例本身
    不绑定任何 loop。
    """

    def __init__(
        self,
        *,
        addr: str | None = None,
        token: str | None = None,
        timeout: float | None = None,
        client: Any = None,
    ) -> None:
        self._addr = addr if addr is not None else settings.milvus_addr
        self._token = token if token is not None else settings.milvus_token
        self._timeout = timeout
        self._client = client
        # 每个事件循环一个 client（弱引用 key：loop 被回收时条目随之消失，
        # 不延长 loop 生命周期）。key 按 loop 天然互斥，_get_client 为同步
        # 代码（同 loop 协程内串行），无需加锁。无 running loop 的调用退回
        # unscoped client（防御路径：正常所有数据面/生命周期方法都在协程内
        # 解析 client，不会走到）。
        self._loop_clients: WeakKeyDictionary[Any, Any] = WeakKeyDictionary()
        self._unscoped_client: Any = None

    def _build_client(self) -> Any:
        from pymilvus import AsyncMilvusClient

        kwargs: dict[str, Any] = {"uri": normalize_milvus_uri(self._addr)}
        if self._token:
            kwargs["token"] = self._token
        if self._timeout is not None:
            kwargs["timeout"] = self._timeout
        return AsyncMilvusClient(**kwargs)

    def _get_client(self) -> Any:
        # 注入 client（单测 fake）直通，不参与分池。
        if self._client is not None:
            return self._client
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            if self._unscoped_client is None:
                self._unscoped_client = self._build_client()
            return self._unscoped_client
        client = self._loop_clients.get(loop)
        if client is None:
            client = self._build_client()
            self._loop_clients[loop] = client
        return client

    async def aclose(self) -> None:
        """释放当前 loop 绑定的底层 gRPC channel（进程退出路径；未连接时 no-op）。

        只关闭调用方所在 loop 的 client；其他 loop 的由对应 loop 上的 aclose
        关闭（main.py shutdown 分别在两个 loop 上调度）。注入 client 语义不变：
        一次性关闭。
        """
        if self._client is not None:
            client, self._client = self._client, None
            await self._safe_close(client)
            return
        client, self._unscoped_client = self._unscoped_client, None
        await self._safe_close(client)
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        await self._safe_close(self._loop_clients.pop(loop, None))

    @staticmethod
    async def _safe_close(client: Any) -> None:
        if client is None:
            return
        try:
            await client.close()
        except Exception as exc:  # noqa: BLE001 — best-effort close
            logger.debug("milvus client close failed (ignored): %s", exc)

    # ── collection 生命周期（CreateKB 建 / DeleteKB 删 / rebuild 重建）──────

    async def create_collection(self, *, name: str, dim: int, metric: str = "COSINE") -> None:
        """CreateKB：建 `kb_{kb_id}` collection（schema 维度与 embedding 对齐）。

        已存在同名 collection 时视为幂等成功（CreateKB 重试路径：上一轮
        collection 建成后 kb 行被清理，重试直接复用同名 collection）。
        """
        if dim <= 0:
            raise VectorStoreError(f"invalid embedding dimension: {dim!r}")
        client = self._get_client()
        try:
            if await client.has_collection(name):
                logger.info("milvus collection %s already exists (idempotent create)", name)
                return
            from pymilvus import DataType

            schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
            schema.add_field("id", DataType.VARCHAR, is_primary=True, max_length=_PK_MAX_LENGTH)
            schema.add_field("vector", DataType.FLOAT_VECTOR, dim=dim)
            schema.add_field("content", DataType.VARCHAR, max_length=_CONTENT_MAX_LENGTH)
            schema.add_field("metadata", DataType.JSON)
            index_params = client.prepare_index_params()
            index_params.add_index(
                field_name="vector", index_type="AUTOINDEX", metric_type=metric
            )
            await client.create_collection(
                name, schema=schema, index_params=index_params
            )
        except Exception as exc:
            raise VectorStoreError(f"create collection {name} failed: {exc}") from exc

    async def drop_collection(self, *, name: str) -> bool:
        """DeleteKB：删 collection。不存在时返回 False（幂等）。"""
        client = self._get_client()
        try:
            if not await client.has_collection(name):
                logger.info("milvus collection %s does not exist (idempotent drop)", name)
                return False
            await client.drop_collection(name)
            return True
        except Exception as exc:
            raise VectorStoreError(f"drop collection {name} failed: {exc}") from exc

    async def recreate_collection(self, *, name: str, dim: int, metric: str = "COSINE") -> None:
        """rebuild：先删后建（schema 维度随当前 embedding 模型重建）。"""
        await self.drop_collection(name=name)
        await self.create_collection(name=name, dim=dim, metric=metric)

    # ── 数据面（insert / search / delete，输入输出形状对齐 Core API）────────

    async def insert(self, *, collection: str, documents: list[dict[str, Any]]) -> None:
        """插入预计算向量。

        documents: [{id, content, vector, metadata}]（Core
        insertVectorStoreDocuments 的 documents 形状）。
        """
        client = self._get_client()
        rows = [
            {
                "id": d["id"],
                "vector": d["vector"],
                "content": d.get("content", "") or "",
                "metadata": d.get("metadata") or {},
            }
            for d in documents
        ]
        try:
            await client.insert(collection_name=collection, data=rows)
        except Exception as exc:
            raise VectorStoreError(f"insert into {collection} failed: {exc}") from exc

    async def search(
        self,
        *,
        collection: str,
        vector: list[float],
        top_k: int,
        filter_expr: str | None = None,
    ) -> list[dict[str, Any]]:
        """向量检索，返回 Core searchVectorStore 的 items 形状。

        每项 {id, score, content, metadata}；COSINE metric 下 distance 即
        余弦相似度，直接作为 score 透传。
        """
        client = self._get_client()
        try:
            results = await client.search(
                collection_name=collection,
                data=[vector],
                limit=top_k,
                filter=filter_expr or "",
                output_fields=["content", "metadata"],
            )
        except Exception as exc:
            raise VectorStoreError(f"search {collection} failed: {exc}") from exc
        items: list[dict[str, Any]] = []
        for hits in results or []:
            for hit in hits or []:
                entity = hit.get("entity") or {}
                items.append(
                    {
                        "id": str(hit.get("id", "")),
                        "score": float(hit.get("distance", 0.0) or 0.0),
                        "content": str(entity.get("content", "") or ""),
                        "metadata": entity.get("metadata") or {},
                    }
                )
        return items

    async def delete(self, *, collection: str, filter_expr: str) -> None:
        """按布尔表达式删除向量（如 `metadata["doc_id"] == "..."`）。"""
        client = self._get_client()
        try:
            await client.delete(collection_name=collection, filter=filter_expr)
        except Exception as exc:
            raise VectorStoreError(
                f"delete from {collection} failed: {exc}"
            ) from exc


# 进程级单例（懒连接，构造无 IO；多租户共享一个 Milvus 连接，租户隔离由
# 每 KB 独立 collection 承载）。单例可被 kb 进程的两个事件循环共享（捕获
# 时刻无妨）：底层 client 按操作时的 running loop 分池，实例本身不绑定
# 任何 loop（见 MilvusVectorStore 类注释）。
_default_vector_store: MilvusVectorStore | None = None


def get_default_vector_store() -> MilvusVectorStore:
    """Return the process-wide MilvusVectorStore singleton (lazy connect)."""
    global _default_vector_store
    if _default_vector_store is None:
        # 直连后无中间层超时兜底：默认超时来自 MILVUS_TIMEOUT（防 Milvus/etcd
        # 挂起时无限等待拖死 Query/parse）。
        _default_vector_store = MilvusVectorStore(timeout=settings.milvus_timeout)
    return _default_vector_store
