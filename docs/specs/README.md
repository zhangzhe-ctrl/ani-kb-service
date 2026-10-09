# docs/specs/ — 行为规格

拆分对象的行为规格目录：核心资源状态机、解析幂等语义、outbox/ack 语义、降级语义、闸门规则等，自阶段 1 起按批次落档。

快照基线（`68bf198f`）下的行为事实以 ADR 0001 §一（1.2-1.4）为权威描述。

## 规格文件

| 文件 | 批次 | 内容 |
|---|---|---|
| [proto-contract.md](proto-contract.md) | KB-SPLIT-P2（#060） | proto 契约固化：单一真源 + pin 约定、Python 生成命令与版本锁定、D2/D3 边界 |
| [semver-tagging.md](semver-tagging.md) | KB-SPLIT-P3B（#062） | 镜像独立版本：SemVer 格式、git tag `kb-v*.*.*` 命名、镜像 tag 映射、CI 触发与格式校验 |
| [observability.md](observability.md) | KB-SPLIT-P3C（#063） | 运行合同保持 + 可观测性：`/readyz` 门控语义、六类领域指标与基数控制、日志关联四要素、JetStream ack 语义 |
