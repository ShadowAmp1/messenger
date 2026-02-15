import importlib.util
from pathlib import Path


def _load_module(monkeypatch, cors_origins=None):
    monkeypatch.setenv("DATABASE_URL", "postgresql://user:pass@localhost:5432/test")
    monkeypatch.setenv("JWT_SECRET", "test-secret")
    monkeypatch.setenv("CLOUDINARY_CLOUD_NAME", "test")
    monkeypatch.setenv("CLOUDINARY_API_KEY", "test")
    monkeypatch.setenv("CLOUDINARY_API_SECRET", "test")

    if cors_origins is None:
        monkeypatch.delenv("CORS_ORIGINS", raising=False)
    else:
        monkeypatch.setenv("CORS_ORIGINS", cors_origins)

    module_path = Path(__file__).resolve().parents[1] / "main.py"
    spec = importlib.util.spec_from_file_location("backend_main_config", module_path)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(module)
    return module


def test_cors_origins_default_localhost(monkeypatch):
    module = _load_module(monkeypatch)

    assert module.CORS_ORIGINS == ["http://localhost"]


def test_cors_origins_csv_parsing(monkeypatch):
    module = _load_module(monkeypatch, "https://app.example.com, http://localhost:5173")

    assert module.CORS_ORIGINS == ["https://app.example.com", "http://localhost:5173"]
