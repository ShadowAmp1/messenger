import importlib.util
from pathlib import Path
from urllib.parse import unquote

import pytest

MODULE_PATH = Path(__file__).resolve().parents[1] / "main.py"


def _load_main_module(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "postgresql://user:pass@localhost:5432/test")
    monkeypatch.setenv("JWT_SECRET", "test-secret-123456")
    monkeypatch.setenv("CLOUDINARY_CLOUD_NAME", "test")
    monkeypatch.setenv("CLOUDINARY_API_KEY", "test")
    monkeypatch.setenv("CLOUDINARY_API_SECRET", "test")

    spec = importlib.util.spec_from_file_location("backend_main_media", MODULE_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(module)
    return module


def _extract_token(access_url: str) -> str:
    return unquote(access_url.split("token=", 1)[1])


def test_media_access_url_contains_signed_token(monkeypatch):
    module = _load_main_module(monkeypatch)

    token = _extract_token(module.build_media_access_url("c1", 42, ttl_seconds=90))
    payload = module._verify_media_token(token)

    assert payload["chat_id"] == "c1"
    assert payload["message_id"] == 42


def test_media_access_rejects_expired_token(monkeypatch):
    module = _load_main_module(monkeypatch)

    expired_token = module._sign_media_token_payload({"chat_id": "c1", "message_id": 7, "exp": 1})

    with pytest.raises(module.HTTPException) as err:
        module._verify_media_token(expired_token)

    assert err.value.status_code == 403


def test_media_access_redirects_to_cloudinary_url(monkeypatch):
    module = _load_main_module(monkeypatch)

    class DummyCursor:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, *_args, **_kwargs):
            return None

        def fetchone(self):
            return {
                "media_url": "https://res.cloudinary.com/demo/image/upload/v1/file.jpg",
                "chat_id": "c1",
                "deleted_for_all": False,
            }

    class DummyConn:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def cursor(self):
            return DummyCursor()

    module.db = lambda: DummyConn()

    token = _extract_token(module.build_media_access_url("c1", 100))
    response = module.access_media(token=token)

    assert response.status_code == 307
    assert response.headers["location"].startswith("https://res.cloudinary.com/")
