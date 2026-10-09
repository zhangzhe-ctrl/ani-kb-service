"""Structured-log correlation helper (issue-063, ADR §五-阶段3).

ADR 要求日志关联 **request / kb / document / run_id** 四要素，使一次上传/解析/
重建/查询链路可从日志中串起来。

kb-service 目前没有 logging 配置（无 dictConfig / 无 ContextVar 过滤器），
按拆分期"平移优先、不做功能性重构"的纪律，不引入日志框架：把四要素渲染成
`key=value` 形式的**结构化后缀**拼进日志消息，既保证可 grep，又不改变
既有日志调用形态。缺省/空值字段一律省略。

- ``request_id``：Gateway 透传的 ``x-request-id``（gRPC metadata），管理面请求
  入口读取；消费者侧无请求上下文，省略。
- ``kb_id`` / ``doc_id``：请求体或消息 payload 携带。
- ``run_id``：解析 run 标识，取 kb_operations 的 ``task_id``（消费者侧可得）。
"""

from __future__ import annotations


def corr(
    *,
    request_id: str | None = None,
    kb_id: str | None = None,
    doc_id: str | None = None,
    run_id: str | None = None,
) -> str:
    """Render the log-correlation suffix for the four ADR elements.

    Returns a space-separated ``key=value`` string; empty/None fields are
    omitted so a bare call yields ``""`` (safe to append unconditionally).
    """
    parts: list[str] = []
    if request_id:
        parts.append(f"request_id={request_id}")
    if kb_id:
        parts.append(f"kb_id={kb_id}")
    if doc_id:
        parts.append(f"doc_id={doc_id}")
    if run_id:
        parts.append(f"run_id={run_id}")
    return " ".join(parts)
