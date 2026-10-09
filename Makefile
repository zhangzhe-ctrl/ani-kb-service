# ANI kb-service — 独立构建入口
#
# 使用方式：make <target>
#
# KB-SPLIT-P3B (issue-062, ADR 0001 §四 #9)：kb-service 拥有自带门禁，
# 不依赖 monorepo 根 Makefile。本文件内所有路径均相对 kb-service 仓库根，
# 门禁脚本位于 ./scripts/，可在独立仓库中原样运行。

# 自动加载 .env（存在则注入，不存在不报错），并导出到子进程环境
-include .env
export

# 镜像构建（对齐 monorepo 根 Makefile 的 REGISTRY / buildx 约定）
REGISTRY ?= harbor.ani.internal/ani
# SemVer 独立版本：优先取 kb 专属 git tag（kb-v1.2.3 → 1.2.3 由 CI 剥离
# kb-v 前缀），本地无 tag 时回落 git describe / dev（详见
# docs/specs/semver-tagging.md）。--match 'kb-v*' 严格限定只认 kb 自己的
# tag，避免误取 monorepo 其他服务的 v*.*.* tag（版本单一真源）。
VERSION ?= $(shell git describe --tags --match 'kb-v*' --always --dirty 2>/dev/null || echo "dev")
IMAGE ?= $(REGISTRY)/kb-service:$(VERSION)
IMAGE_BUILD := docker buildx build
IMAGE_BUILD_FLAGS := --load --sbom=true --provenance=mode=max

# 归一化 Python 解释器与校验脚本路径
PYTHON ?= python
SCRIPTS := scripts

# lint 规则子集：pyflakes 致命错误 + 语法错误（E9/F63/F7/F82）。
# kb-service 存在大量预存 lint 债，门禁仅守编译/未定义名/未用导入等
# 硬错误，保证流水线全绿且不引入噪声。protobuf 生成物与 e2e 排除。
RUFF_SELECT := E9,F63,F7,F82
RUFF_IGNORE := E402
RUFF_EXCLUDES := --exclude app/generated --exclude app/rag_engine/rag_pb2.py \
	--exclude app/rag_engine/rag_pb2_grpc.py --exclude app/rag_engine/rag_pb2.pyi \
	--exclude .venv --exclude tests/e2e

.PHONY: help test test-python lint lint-python \
        validate-architecture validate-sql-centralization validate-pg-init-consistency \
        validate-migrations image clean

# ── 帮助 ──────────────────────────────────────────────────────────────────────
help:
	@echo "ANI kb-service 构建命令"
	@echo ""
	@echo "  门禁:"
	@echo "    make lint                    ruff 硬错误子集 + mypy scripts"
	@echo "    make validate-architecture   SQL 集中化 + init/migration 一致性"
	@echo "    make validate-migrations     migration baseline 结构校验"
	@echo "    make test-python             pytest（tests/）"
	@echo "    make test                    lint + validate-architecture + validate-migrations + test-python"
	@echo ""
	@echo "  镜像:"
	@echo "    make image                   构建 $(IMAGE)（VERSION=$(VERSION)）"
	@echo ""
	@echo "    make clean                   清理 __pycache__ / .pytest_cache"

# ── 门禁总入口 ────────────────────────────────────────────────────────────────
test: lint validate-architecture validate-migrations test-python
	@echo "✅ kb-service gate valid"

# ── 测试 ──────────────────────────────────────────────────────────────────────
test-python:
	@echo "→ kb-service pytest..."
	$(PYTHON) -m pytest -q
	@echo "✅ kb-service tests valid"

# ── 代码检查 ──────────────────────────────────────────────────────────────────
lint: lint-python

lint-python:
	@echo "→ kb-service ruff（$(RUFF_SELECT)）..."
	$(PYTHON) -m ruff check --select $(RUFF_SELECT) --ignore $(RUFF_IGNORE) $(RUFF_EXCLUDES) .
	@echo "→ kb-service mypy（门禁脚本）..."
	$(PYTHON) -m mypy --ignore-missing-imports $(SCRIPTS)
	@echo "✅ kb-service lint valid"

# ── 架构门禁 ──────────────────────────────────────────────────────────────────
validate-architecture: validate-sql-centralization validate-pg-init-consistency
	@echo "✅ kb-service architecture guardrails valid"

validate-sql-centralization:
	@echo "→ 校验 kb 表 SQL 仅存在于 app/repositories/..."
	$(PYTHON) $(SCRIPTS)/validate_kb_sql_centralization.py

validate-pg-init-consistency:
	@echo "→ 校验 kb 专属 PG 角色基线 migrations/000 与 init-scripts/00 的一致性..."
	$(PYTHON) $(SCRIPTS)/validate_kb_pg_init_consistency.py

# ── migration 基线门禁 ────────────────────────────────────────────────────────
validate-migrations:
	@echo "→ 校验 migration baseline 结构（NNN_<name>.sql，000 起无 gap/dup）..."
	$(PYTHON) $(SCRIPTS)/validate_kb_migrations.py

# ── 镜像构建 ──────────────────────────────────────────────────────────────────
image: validate-architecture validate-migrations
	@echo "→ 构建 kb-service 镜像 $(IMAGE)..."
	$(IMAGE_BUILD) $(IMAGE_BUILD_FLAGS) -t $(IMAGE) -f Dockerfile .
	@echo "✅ kb-service image built: $(IMAGE)"

# ── 清理 ──────────────────────────────────────────────────────────────────────
clean:
	@echo "→ 清理 Python 缓存..."
	$(PYTHON) -c "import pathlib, shutil; [shutil.rmtree(p, ignore_errors=True) for p in pathlib.Path('.').rglob('__pycache__') if '.venv' not in p.parts]; [shutil.rmtree(p, ignore_errors=True) for p in ('.pytest_cache', '.mypy_cache', '.ruff_cache')]"
	@echo "✅ cleaned"
