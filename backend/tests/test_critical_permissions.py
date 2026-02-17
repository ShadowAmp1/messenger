import asyncio
import importlib.util
from pathlib import Path

import pytest
from fastapi import Response

MODULE_PATH = Path(__file__).resolve().parents[1] / "main.py"


def _load_main_module(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "postgresql://user:pass@localhost:5432/test")
    monkeypatch.setenv("JWT_SECRET", "test-secret-123456")
    monkeypatch.setenv("CLOUDINARY_CLOUD_NAME", "test")
    monkeypatch.setenv("CLOUDINARY_API_KEY", "test")
    monkeypatch.setenv("CLOUDINARY_API_SECRET", "test")

    spec = importlib.util.spec_from_file_location("backend_main_critical", MODULE_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(module)
    return module


def test_refresh_rotation_sets_cookie_and_rotates_token(monkeypatch):
    module = _load_main_module(monkeypatch)

    now = 1_700_000_111
    store = {
        "old-rt": {
            "username": "alice",
            "session_id": "sess-1",
            "expires_at": now + 3600,
            "revoked": False,
            "replaced_by": None,
        }
    }

    class DummyRequest:
        cookies = {module.REFRESH_COOKIE_NAME: "old-rt"}

    class DummyCursor:
        def __init__(self):
            self._row = None

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, query, params=None):
            if query.startswith("SELECT username, expires_at, revoked, session_id, replaced_by FROM refresh_tokens"):
                self._row = store.get(params[0])
            elif query.startswith("INSERT INTO refresh_tokens"):
                token, username, session_id, _created_at, expires_at = params
                store[token] = {
                    "username": username,
                    "session_id": session_id,
                    "expires_at": expires_at,
                    "revoked": False,
                    "replaced_by": None,
                }
            elif query.startswith("UPDATE refresh_tokens SET revoked=TRUE, replaced_by="):
                new_rt, old_rt = params
                store[old_rt]["revoked"] = True
                store[old_rt]["replaced_by"] = new_rt

        def fetchone(self):
            return self._row

    class DummyConn:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def cursor(self):
            return DummyCursor()

        def commit(self):
            return None

    module.db = lambda: DummyConn()
    module.now_ts = lambda: now
    module.secrets.token_urlsafe = lambda _n: "new-rt"
    module.check_auth_rate_limit = lambda request, action: None

    response = Response()
    payload = module.refresh_tokens(DummyRequest(), response)

    assert payload["username"] == "alice"
    assert isinstance(payload["token"], str)
    assert store["old-rt"]["revoked"] is True
    assert store["old-rt"]["replaced_by"] == "new-rt"
    assert store["new-rt"]["session_id"] == "sess-1"
    assert "refresh_token=new-rt" in response.headers.get("set-cookie", "")


def test_create_message_requires_member(monkeypatch):
    module = _load_main_module(monkeypatch)

    class DummyConn:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    module.db = lambda: DummyConn()
    module.check_rate_limit = lambda key, limit: None

    called = {"value": False}

    def _require_member(conn, chat_id, username):
        called["value"] = True
        raise module.HTTPException(status_code=403, detail="Not a member")

    module.require_member = _require_member

    with pytest.raises(module.HTTPException) as err:
        asyncio.run(
            module.create_text_message(
                type("DummyMessage", (), {"chat_id": "c1", "text": "hello", "reply_to_id": 0})(),
                username="alice",
            )
        )

    assert called["value"] is True
    assert err.value.status_code == 403


def _build_message_db(module, sender="alice"):
    class DummyCursor:
        def __init__(self):
            self._row = None

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, query, params=None):
            if query.startswith("SELECT chat_id, sender, deleted_for_all, deleted_at FROM messages"):
                self._row = {
                    "chat_id": "c1",
                    "sender": sender,
                    "deleted_for_all": False,
                    "deleted_at": None,
                }
            elif query.startswith("SELECT chat_id, sender FROM messages"):
                self._row = {"chat_id": "c1", "sender": sender}

        def fetchone(self):
            return self._row

    class DummyConn:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def cursor(self):
            return DummyCursor()

        def commit(self):
            return None

    module.db = lambda: DummyConn()


def test_edit_message_denies_non_sender(monkeypatch):
    module = _load_main_module(monkeypatch)
    _build_message_db(module, sender="bob")
    module.require_member = lambda conn, chat_id, username: None

    with pytest.raises(module.HTTPException) as err:
        asyncio.run(module.edit_message(1, module.MessageEditIn(text="updated"), username="alice"))

    assert err.value.status_code == 403
    assert err.value.detail == "Only sender can edit"


def test_delete_message_all_denies_non_sender(monkeypatch):
    module = _load_main_module(monkeypatch)
    _build_message_db(module, sender="bob")
    module.require_member = lambda conn, chat_id, username: None

    with pytest.raises(module.HTTPException) as err:
        asyncio.run(module.delete_message(1, scope="all", username="alice"))

    assert err.value.status_code == 403
    assert err.value.detail == "Only sender can delete for all"
