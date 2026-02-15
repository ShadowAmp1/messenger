import importlib.util
from pathlib import Path

import pytest

MODULE_PATH = Path(__file__).resolve().parents[1] / "main.py"


def _base_env(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "postgresql://user:pass@localhost:5432/test")
    monkeypatch.setenv("JWT_SECRET", "test-secret-123456")
    monkeypatch.setenv("CLOUDINARY_CLOUD_NAME", "test")
    monkeypatch.setenv("CLOUDINARY_API_KEY", "test")
    monkeypatch.setenv("CLOUDINARY_API_SECRET", "test")


def _import_main_module():
    spec = importlib.util.spec_from_file_location("backend_main_required_env", MODULE_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(module)
    return module


def test_missing_jwt_secret_fails_fast(monkeypatch):
    _base_env(monkeypatch)
    monkeypatch.delenv("JWT_SECRET", raising=False)

    with pytest.raises(RuntimeError, match="JWT_SECRET env is required"):
        _import_main_module()


def test_missing_database_url_fails_fast(monkeypatch):
    _base_env(monkeypatch)
    monkeypatch.delenv("DATABASE_URL", raising=False)

    with pytest.raises(RuntimeError, match="DATABASE_URL env is required"):
        _import_main_module()


def test_short_jwt_secret_fails_fast(monkeypatch):
    _base_env(monkeypatch)
    monkeypatch.setenv("JWT_SECRET", "short")

    with pytest.raises(RuntimeError, match="JWT_SECRET must be at least 16 characters"):
        _import_main_module()


@pytest.mark.parametrize(
    "missing_var",
    ["CLOUDINARY_CLOUD_NAME", "CLOUDINARY_API_KEY", "CLOUDINARY_API_SECRET"],
)
def test_missing_cloudinary_env_fails_fast(monkeypatch, missing_var):
    _base_env(monkeypatch)
    monkeypatch.delenv(missing_var, raising=False)

    with pytest.raises(RuntimeError, match="Cloudinary env vars required"):
        _import_main_module()
