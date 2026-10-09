# docs/execution/records/ — 过程记录存档

每批次的过程记录与协作排期知会/回执存档。执行状态汇总见 [../status.md](../status.md)。

## KB-SPLIT-P0（2026-09-21，阶段 0 基线固定）

- `KB-SPLIT-P0-notice-frontend.md` — 协作排期知会 ①：前端上传改 kb 预签名直传（裁定 D1）
- `KB-SPLIT-P0-notice-gateway.md` — 协作排期知会 ②：Gateway kb 路由改指向独立 kb-service
- `KB-SPLIT-P0-notice-task.md` — 协作排期知会 ③：task 拆分相关方（kb outbox 事件 + 临时进度查询接口，裁定 D4）

## KB-SPLIT-P4B（2026-10-08，Core 调用面清零）

- `KB-SPLIT-P4B-notice-core.md` — 协作排期知会 ④：向 Core 团队发出端点下线知会（vector-stores / knowledge-base-link / 预计算向量插入，ADR §D1/§D3/§四#6/§五-阶段5）
