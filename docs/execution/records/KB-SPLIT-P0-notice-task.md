# 协作排期知会 ③ task 拆分相关方：kb outbox 事件契约 + 临时进度查询接口（裁定 D4）

- **发出**：2026-09-21（批次 KB-SPLIT-P0，基线 commit `68bf198f`）
- **对象**：task 拆分相关方（未来 task-service 团队 + Gateway task 路由维护方）
- **依据**：`docs/adr/0001-split-kb-from-core.md` §二-D4、§五-阶段1/阶段6、§六（两次拆分互不阻塞）

## 变更内容

| 项 | 现状 | 拆分后 |
|---|---|---|
| 幂等回放 | CreateKB/UpdateKB/Reparse 等重复请求判定写**共享 async_tasks** | kb 新建 `kb_operations`（`UNIQUE(tenant_id, idempotency_key)`）本地化；**不再写共享 async_tasks** |
| 事件投递 | kb 经共享 outbox_events + dispatcher 轮询 | kb **自建自有 outbox 表**（结构沿用现设计，pending→published 状态机不变），事件 schema 稳定 + **at-least-once 投递承诺** |
| 任务进度对外可见 | Gateway task 路由读共享 async_tasks | kb 只发 outbox 事件；task-service 订阅 kb 事件建任务、承接展示。**过渡期** kb 提供 `ListOperations` / `GetOperation` gRPC（读 kb_operations）作为临时进度查询接口 |
| 解耦接口 | — | kb 对 task 侧承诺的只有「outbox 事件 schema 稳定 + at-least-once」；task-service 依赖事件而非 kb 的表 |

## 需要 task 相关方做的

1. 确认 kb outbox 事件消费方式（subject 命名沿用 `ani.tasks.kb.*`）与临时查询接口（ListOperations/GetOperation，#052 交付）的聚合方式。
2. task-service 就绪前：任务展示的临时来源是 kb 的查询接口；就绪后（阶段 6）切到「订阅 kb 事件 + Gateway task 路由接 task-service」。
3. 回执本知会时确认事件 schema 评审窗口。

## 时间窗口

- kb 侧 outbox 自建 + kb_operations（#050/#051）与查询接口（#052）在**阶段 1** 交付；两次拆分（kb / task）互不阻塞。

## 交付登记（#052，2026-09-22）

`ListOperations` / `GetOperation` gRPC 已随 KB-SPLIT-P1C 交付（`api/proto/kb/v1/kb_service.proto` + kb-service servicer，读 kb_operations）。**定位：task-service 就绪前的临时进度查询接口**——演进期内由调用方（Gateway/运维）直接查询 kb 操作状态；task-service 就绪后（ADR 0001 §五-阶段6）由其**订阅 kb outbox 事件承接任务展示**，本接口退居运维排查用途，下线时机随 #069（task 域/Gateway 最终对接）一并收敛。

## 回执记录

| 日期 | 回执方 | 结论 |
|---|---|---|
| 2026-09-21 | — | 知会发出，待回复 |
