"""Tests for kb-service configuration (issue-006 / SPEC §2.4).

KB-SPLIT-P3A (issue-061, ADR 0001 §四 #5 / §五-阶段3): kb-service loads its
own `.env` (not the monorepo shared root). Verifies the Settings model
still loads without crashing on unrelated env vars (AUTH_*, GATEWAY_PORT,
etc. belong to other ANI services, ``extra=ignore``), that the field names
map to the project env conventions (DATABASE_URL / NATS_URL / REDIS_URL)
and the kb-dedicated KB_EMBEDDING_MODEL /
KB_EMBEDDING_DIM (legacy shared EMBEDDING_MODEL / EMBEDDING_DIM kept
readable via AliasChoices), and that malformed config fails fast.
"""
import os
import sys

import pytest

_SERVICE_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, _SERVICE_ROOT)

from app.core.config import Settings


def test_settings_instantiate_with_extra_env_vars_ignored():
    """Shared .env has many unrelated keys; kb-service must not crash (extra=ignore)."""
    # Simulate a shared .env with unrelated keys plus kb-service keys.
    env = {
        "AUTH_JWT_ISSUER": "ani-dev",             # not for kb-service
        "GATEWAY_PORT": "8080",                   # not for kb-service
        "GRPC_PORT": "50199",                     # kb-service
        "DATABASE_URL": "postgres://u:p@h:5432/d",  # kb-service
        "NATS_URL": "nats://h:4222",              # kb-service
        "REDIS_URL": "redis://h:6379/0",          # kb-service
    }
    with _patch_env(env):
        s = Settings()
    assert s.grpc_port == 50199
    assert s.database_url == "postgres://u:p@h:5432/d"
    assert s.nats_url == "nats://h:4222"
    assert s.redis_url == "redis://h:6379/0"


def test_settings_minio_keys_follow_env():
    """KB-SPLIT-P1I (issue-058, ADR 0001 §D1): MINIO_* env vars must map
    onto the kb-service MinIO settings (shared .env keys, case-insensitive
    pydantic-settings) — presigned direct upload to kb's own bucket."""
    env = {
        "MINIO_ENDPOINT": "10.0.0.10:9000",
        "MINIO_PUBLIC_ENDPOINT": "10.10.1.66:31950",
        "MINIO_ACCESS_KEY": "ak",
        "MINIO_SECRET_KEY": "sk",
        "MINIO_USE_SSL": "true",
        "MINIO_BUCKET": "kb-docs-prod",
        "MINIO_PRESIGN_TTL_SECONDS": "600",
    }
    with _patch_env(env):
        s = Settings(_env_file=None)
    assert s.minio_endpoint == "10.0.0.10:9000"
    assert s.minio_public_endpoint == "10.10.1.66:31950"
    assert s.minio_access_key == "ak"
    assert s.minio_secret_key == "sk"
    assert s.minio_secure is True
    assert s.minio_bucket == "kb-docs-prod"
    assert s.minio_presign_ttl_seconds == 600


def test_settings_minio_defaults():
    """No env override → kb's own bucket defaults (e2e compose precedent
    bucket name; fixed region makes presign purely local; 15-minute TTL
    mirroring the Core Go precedent)."""
    env = {k: None for k in (
        "MINIO_ENDPOINT", "MINIO_PUBLIC_ENDPOINT", "MINIO_ACCESS_KEY",
        "MINIO_SECRET_KEY", "MINIO_USE_SSL", "MINIO_BUCKET",
        "MINIO_PRESIGN_TTL_SECONDS",
    )}
    with _patch_env(env):
        s = Settings(_env_file=None)
    assert s.minio_endpoint == "localhost:9000"
    assert s.minio_public_endpoint == ""
    assert s.minio_bucket == "ani-kb-docs"
    assert s.minio_region == "us-east-1"
    assert s.minio_presign_ttl_seconds == 900
    assert s.minio_secure is False


def test_settings_default_database_url_points_to_kb_dedicated_instance():
    """KB-SPLIT-P1F (issue-055, ADR 0001 §四 #1): the default connection
    string must point at the kb-dedicated PostgreSQL instance (port 5433,
    database kb) using the least-privilege application role ani_app_user —
    not the shared library (localhost:5432/ani)."""
    with _patch_env({"DATABASE_URL": None}):
        s = Settings(_env_file=None)
    assert s.database_url == (
        "postgresql://ani_app_user:ani_app_dev_password@localhost:5433/kb"
    )


def test_settings_embedding_model_has_no_default():
    """No env override → embedding_model is empty ("unset"), NOT a hardcoded
    default model. CreateKB falls back to this value when the request omits
    embedding_model and rejects the call (INVALID_ARGUMENT) when both are
    empty, instead of silently using a default. embedding_dim keeps 1024 as
    the probe-failure fallback (not the dimension source of truth)."""
    # _env_file=None 只禁 .env 文件，os.environ 仍会被 pydantic-settings 读取；
    # 显式清除新旧两套键（宿主机/CI 若导出过会让默认值断言假阳性失败）。
    with _patch_env({k: None for k in (
        "KB_EMBEDDING_MODEL", "KB_EMBEDDING_DIM",
        "EMBEDDING_MODEL", "EMBEDDING_DIM",
    )}):
        s = Settings(_env_file=None)
    assert s.embedding_model == ""
    assert s.embedding_dim == 1024


def test_settings_embedding_keys_follow_kb_dedicated_env():
    """KB-SPLIT-P3A (issue-061): the kb-dedicated KB_EMBEDDING_MODEL /
    KB_EMBEDDING_DIM env vars override the defaults — CreateKB's fallback
    and the vector-store dimension follow kb's own keys."""
    env = {"KB_EMBEDDING_MODEL": "Qwen/Qwen3-Embedding-4B", "KB_EMBEDDING_DIM": "2560"}
    with _patch_env(env):
        s = Settings(_env_file=None)
    assert s.embedding_model == "Qwen/Qwen3-Embedding-4B"
    assert s.embedding_dim == 2560


def test_settings_embedding_keys_read_legacy_shared_env():
    """AliasChoices keeps the legacy shared EMBEDDING_MODEL / EMBEDDING_DIM
    readable while kb migrates to its own keys (shared-environment
    compatibility; same AliasChoices precedent as minio_secure)."""
    env = {"EMBEDDING_MODEL": "Qwen/Qwen3-Embedding-4B", "EMBEDDING_DIM": "2560"}
    with _patch_env(env):
        s = Settings(_env_file=None)
    assert s.embedding_model == "Qwen/Qwen3-Embedding-4B"
    assert s.embedding_dim == 2560


def test_settings_kb_dedicated_embedding_key_wins_over_legacy():
    """When both keys are present, the kb-dedicated key takes precedence
    (declared first in AliasChoices)."""
    env = {
        "KB_EMBEDDING_MODEL": "kb-model",
        "EMBEDDING_MODEL": "legacy-model",
        "KB_EMBEDDING_DIM": "512",
        "EMBEDDING_DIM": "1024",
    }
    with _patch_env(env):
        s = Settings(_env_file=None)
    assert s.embedding_model == "kb-model"
    assert s.embedding_dim == 512


def test_settings_malformed_embedding_dim_fails_fast():
    """Malformed config must fail fast (pydantic validation error), not
    silently coerce to the default — a non-integer dimension is rejected
    at construction time."""
    with _patch_env({"KB_EMBEDDING_DIM": "not-an-int"}):
        with pytest.raises(Exception):
            Settings(_env_file=None)


import contextlib


@contextlib.contextmanager
def _patch_env(env):
    """Set env vars from ``env``; a None value clears (pops) the var."""
    old = {}
    for k, v in env.items():
        old[k] = os.environ.get(k)
        if v is None:
            os.environ.pop(k, None)
        else:
            os.environ[k] = v
    try:
        yield
    finally:
        for k, v in old.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v
