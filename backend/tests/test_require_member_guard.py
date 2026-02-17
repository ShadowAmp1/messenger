import importlib.util
from pathlib import Path

import pytest

MODULE_PATH = Path(__file__).resolve().parents[1] / "main.py"


def _load_main_module(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "postgresql://user:pass@localhost:5432/test")
    monkeypatch.setenv("JWT_SECRET", "test-secret-123456")
    monkeypatch.setenv("CLOUDINARY_CLOUD_NAME", "test")
    monkeypatch.setenv("CLOUDINARY_API_KEY", "test")
    monkeypatch.setenv("CLOUDINARY_API_SECRET", "test")

    spec = importlib.util.spec_from_file_location("backend_main_guard", MODULE_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(module)
    return module


def test_require_member_allows_chat_member(monkeypatch):
    module = _load_main_module(monkeypatch)
    module.is_member = lambda conn, chat_id, username: True

    module.require_member(conn=object(), chat_id="c1", username="alice")


def test_require_member_denies_non_member(monkeypatch):
    module = _load_main_module(monkeypatch)
    module.is_member = lambda conn, chat_id, username: False

    with pytest.raises(module.HTTPException) as err:
        module.require_member(conn=object(), chat_id="c1", username="alice")

    assert err.value.status_code == 403
    assert err.value.detail == "Not a member"


def test_list_messages_uses_require_member_guard(monkeypatch):
    module = _load_main_module(monkeypatch)

    class DummyConn:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    module.db = lambda: DummyConn()

    called = {"value": False}

    def _require_member(conn, chat_id, username):
        called["value"] = True
        raise module.HTTPException(status_code=403, detail="Not a member")

    module.require_member = _require_member

    with pytest.raises(module.HTTPException):
        module.list_messages(chat_id="c1", username="alice")

    assert called["value"] is True
