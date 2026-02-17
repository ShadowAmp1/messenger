import importlib.util
from pathlib import Path

MODULE_PATH = Path(__file__).resolve().parents[1] / "main.py"


def _load_main_module(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "postgresql://user:pass@localhost:5432/test")
    monkeypatch.setenv("JWT_SECRET", "test-secret-123456")
    monkeypatch.setenv("CLOUDINARY_CLOUD_NAME", "test")
    monkeypatch.setenv("CLOUDINARY_API_KEY", "test")
    monkeypatch.setenv("CLOUDINARY_API_SECRET", "test")

    spec = importlib.util.spec_from_file_location("backend_main_pagination", MODULE_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(module)
    return module


def test_normalize_messages_limit_bounds(monkeypatch):
    module = _load_main_module(monkeypatch)

    assert module.normalize_messages_limit(0) == 1
    assert module.normalize_messages_limit(1) == 1
    assert module.normalize_messages_limit(50) == 50
    assert module.normalize_messages_limit(999) == 200
