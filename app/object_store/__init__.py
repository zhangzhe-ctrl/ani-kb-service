"""kb-service 自有 MinIO 对象存储（KB-SPLIT-P1I, issue-058, ADR 0001 §D1）。

D1 裁定：对象由 kb 自管——kb 新建自有 bucket（无存量移交），正本
key `{kb_id}/{doc_id}`，预签名 PUT 直传不经 Core objects 中转。presign
走 publicEndpoint/NodePort 对外视角，stat 走 internal/ClusterIP 集群内
视角，region 固定 us-east-1 使 presign 为纯本地签名。
"""
from app.object_store.minio_client import (
    MinioObjectStore,
    ObjectNotFoundError,
    ObjectStoreError,
    get_default_object_store,
)

__all__ = [
    "MinioObjectStore",
    "ObjectNotFoundError",
    "ObjectStoreError",
    "get_default_object_store",
]
