"""kb-service 直连独立 Milvus 向量库（KB-SPLIT-P1G, issue-056, ADR 0001 §D1）。

D1 裁定：向量由 kb 自管——pymilvus 进 kb 进程，直连独立 Milvus 实例
insert/search/delete，不经 Core vector-stores 登记/中转。collection 随
CreateKB 建立（`kb_{kb_id}` 派生名）、DeleteKB 随删、rebuild 重建，
无存量迁移。
"""
from app.vector_store.milvus_client import (
    MilvusVectorStore,
    VectorStoreError,
    get_default_vector_store,
)

__all__ = [
    "MilvusVectorStore",
    "VectorStoreError",
    "get_default_vector_store",
]
