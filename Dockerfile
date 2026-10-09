# kb-service — 自管向量（Milvus）与对象（MinIO bucket）的 KB 领域服务，
# 经 gRPC 依赖无状态 rag-engine；不调用 Core（ADR 0001 D1/D3）。
FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# 非 root 运行（安全加固）：运行时不落盘，仅需读代码/写 /tmp，故用无特权
# 用户；uid/gid 固定便于 K8s securityContext 对齐。
RUN groupadd --gid 10001 app && useradd --uid 10001 --gid app --no-create-home app \
    && chown -R app:app /app
USER app

# 双栈单进程入口（ADR 0001 §五-阶段3）：uvicorn/FastAPI :8002（health/readyz/debug）
# + gRPC :50053（业务 RPC），两者由 main.py 同一进程启动。
EXPOSE 8002 50053

CMD ["python", "main.py"]
