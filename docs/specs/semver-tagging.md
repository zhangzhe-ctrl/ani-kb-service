# 镜像版本与 SemVer tag 规则（KB-SPLIT-P3B / issue #062）

> 本文件是 kb-service 拆分事务中**镜像独立版本**的落地说明：SemVer 格式、
> git tag 命名、镜像 tag 映射、CI 触发与校验。执行期权威裁定见
> [ADR 0001](../adr/0001-split-kb-from-core.md) §三-1（独立构建/独立版本）、
> §四 #9（monorepo 构建/CI 耦合点）、§五-阶段3（独立 CI：镜像流水线迁出
> monorepo；镜像独立 tag（SemVer））。冲突时以 ADR 为准，本文件随之修订。

## 1. 目标（ADR §三-1）

"三独立"中的**独立构建**与**独立版本**：kb-service 镜像不再随 monorepo
统一入口发布，而是由 kb 独立流水线构建推送，并以 SemVer 独立打 tag，与
服务自身发布节奏解耦。

## 2. SemVer 格式

镜像版本采用语义化版本 **MAJOR.MINOR.PATCH**，形如 `1.0.0`：

| 段 | 递增条件 |
|---|---|
| MAJOR | 不兼容的对外契约变更（`kb.v1` gRPC 契约破坏性变更等） |
| MINOR | 向后兼容的功能新增（新增 RPC、新增能力，不改既有语义） |
| PATCH | 向后兼容的缺陷修复（bug fix，不改契约） |

首个正式版本为 **`v1.0.0`**。预发布/构建元数据后缀（`-rc.1`、`+build`）
按需扩展，但**镜像 tag 主段必须匹配** `^[0-9]+\.[0-9]+\.[0-9]+$`（CI 强校验，
见 §4）。

## 3. git tag 与镜像 tag 映射

- **git tag 命名**：`kb-v<MAJOR>.<MINOR>.<PATCH>`，例如 `kb-v1.0.0`。
  `kb-` 前缀用于与 monorepo 其他服务的 `v*.*.*` tag 区隔，避免误触发
  monorepo 的 `build-image.yml`（该 workflow 只匹配 `v*.*.*`）。
- **镜像 tag**：去掉 `kb-v` 前缀后的 SemVer 段，推送至
  `harbor.ani.internal/ani/kb-service:<MAJOR>.<MINOR>.<PATCH>`，并附带
  `:latest`。
- **本地构建**：`make image` 的 `VERSION` 优先取 `git describe --tags
  --match 'kb-v*'`（**只认 kb 自己的 tag**，避免误取 monorepo 其他服务的
  `v*.*.*`）；无匹配 tag 时回落 `dev`
  （`IMAGE=harbor.ani.internal/ani/kb-service:$(VERSION)`）。

| 来源 | git tag | 镜像 tag |
|---|---|---|
| 正式发布 | `kb-v1.2.3` | `kb-service:1.2.3` + `kb-service:latest` |
| 手动派发 | workflow_dispatch `tag=1.2.3` | `kb-service:1.2.3` + `kb-service:latest` |
| 本地开发 | 无 / 任意 | `kb-service:$(VERSION)`（`dev` 或 describe 结果） |

## 4. CI 触发与 SemVer 校验

独立流水线 [kb-service-ci.yml](../../../../.github/workflows/kb-service-ci.yml)：

1. **门禁 job（`gate`）**：任何 `services/kb-service/**` 的 push / PR 触发；
   安装钉死依赖后执行 kb 自带门禁 `make test`（lint → validate-architecture
   → validate-migrations → test-python）与 `git diff --check`。**不构建镜像**。
2. **镜像 job（`build-image`）**：仅在 push `kb-v*.*.*` tag 或
   workflow_dispatch 时运行（`needs: gate`）。构建前先做 SemVer 格式校验：
   ```
   ^[0-9]+\.[0-9]+\.[0-9]+$   # 不匹配即 fail
   ```
   校验通过后 buildx 多架构构建推送、Trivy 阻断 HIGH/CRITICAL、Cosign 签名，
   并上传 digest 证据 artifact。

## 5. 版本单一真源

- 镜像版本**以 git tag 为单一真源**；`main.py` 中 `FastAPI(version="1.0.0")`
  仅作应用内元数据，不驱动镜像 tag。
- 版本变更 = 打新 tag；**不回溯改写已发布 tag**（不可变发布物）。
- kb 与 monorepo 其他服务各自独立版本，互不约束。
