# 协作排期知会 ① 前端：文档上传改为 kb 签发预签名直传（裁定 D1）

- **发出**：2026-09-21（批次 KB-SPLIT-P0，基线 commit `68bf198f`）
- **对象**：前端团队（Console 知识库模块）
- **依据**：`docs/adr/0001-split-kb-from-core.md` §二-D1（对象自管 + 预签名直传）、§五-阶段1（存储基线）、阶段2（上传契约固化）

## 变更内容

| 项 | 现状 | 拆分后 |
|---|---|---|
| 上传通道 | 前端调 Core 上传端点（objects 链路），Core 通知 kb（NotifyDocumentUploaded RPC） | 前端经 Gateway 请求 **kb 签发预签名 PUT URL**（MinIO NodePort 对外地址、时效受限、key 预定）→ **前端直传 MinIO** → 前端调 **kb complete 端点**（HeadObject 校验 + 登记 kb_documents） |
| 相关端点 | Core vector-stores / objects / knowledge-base-link | 上述 Core 端点不再出现；上传/取用契约全部由 kb 提供 |
| 上传体验 | 两步（上传 + 等待） | 三步（请求签名 → 直传 → complete），对用户单次上传语义不变 |

## 需要前端做的

1. 上传逻辑改造：从「调 Core 上传端点」改为「kb 签发预签名 → 直传 → complete」三段式（**阶段 2 上传契约固化后按新契约接入**）。
2. 关注契约项：预签名 URL 时效、key 规则、双视角 URL（对外 NodePort / 对内 ClusterIP）——阶段 2 交付并列入契约测试。
3. 回执本知会时反馈前端改造排期窗口。

## 时间窗口

- kb 侧存储基线（阶段 1，issue #058/#059）建成即具备后端能力；阶段 1 完成前现状不变、无行为差异。
- 前端改造不阻塞 kb 拆分关键路径（阶段 2 依赖前端排期确认，阶段 4 全链路验收含预签名直传专项）。

## 回执记录

| 日期 | 回执方 | 结论 |
|---|---|---|
| 2026-09-21 | — | 知会发出，待回复 |
