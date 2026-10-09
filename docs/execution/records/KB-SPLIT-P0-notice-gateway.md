# 协作排期知会 ② Gateway：kb 路由改指向独立 kb-service

- **发出**：2026-09-21（批次 KB-SPLIT-P0，基线 commit `68bf198f`）
- **对象**：Gateway 团队（ani-gateway）
- **依据**：`docs/adr/0001-split-kb-from-core.md` §1.6、§五-阶段5（仓库拆分与切换）

## 变更内容

| 项 | 现状 | 拆分后 |
|---|---|---|
| kb 资源路由 | `internal/router/kb_resources.go` 经 `kb_grpc_client.go` 转发到 monorepo 内 kb-service gRPC :50053（含 SSE 流转换 `kb_sse.go`） | 同一套 gRPC 代理代码**改指向独立 kb-service** 的 gRPC 端点；路由/契约形状不变 |
| task 资源路由 | `task_resources.go` 读共享 async_tasks | kb 不再写共享表；任务展示归 task 域（见知会③），Gateway task 路由届时接 task-service |

## 需要 Gateway 做的

1. kb 路由切换是**配置/指向级改动**（现状已是纯 gRPC 代理、不直连 kb 表），按独立 kb-service 的 gRPC 端点调整目标地址与凭据。
2. 切换时点：**阶段 5**（#069 KB-SPLIT-P5B），依赖阶段 4 独立环境全链路验收通过；此前无动作。
3. 回执本知会时确认切换窗口与发布方式（镜像/配置）。

## 时间窗口

- 阶段 0-4 期间 Gateway 零改动；阶段 5 一次性切换（回滚 = 指向回退）。

## 回执记录

| 日期 | 回执方 | 结论 |
|---|---|---|
| 2026-09-21 | — | 知会发出，待回复 |
