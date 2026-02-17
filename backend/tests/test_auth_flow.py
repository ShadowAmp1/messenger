import importlib.util
from pathlib import Path

import pytest
from fastapi import Response


def _load_main_module(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "postgresql://user:pass@localhost:5432/test")
    monkeypatch.setenv("JWT_SECRET", "test-secret-123456")
    monkeypatch.setenv("CLOUDINARY_CLOUD_NAME", "test")
    monkeypatch.setenv("CLOUDINARY_API_KEY", "test")
    monkeypatch.setenv("CLOUDINARY_API_SECRET", "test")

    module_path = Path(__file__).resolve().parents[1] / "main.py"
    spec = importlib.util.spec_from_file_location("backend_main", module_path)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(module)
    return module


def test_register_then_login(monkeypatch):
    module = _load_main_module(monkeypatch)

    users = {}

    class DummyRequest:
        class Client:
            host = "127.0.0.1"

        client = Client()

    class DummyCursor:
        def __init__(self):
            self._row = None

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, query, params=None):
            if query.startswith("INSERT INTO users"):
                username, pass_hash, _created_at = params
                users[username] = {"pass_hash": pass_hash}
            elif query.startswith("SELECT pass_hash FROM users"):
                username = params[0]
                self._row = users.get(username)

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
    module.ensure_favorites_for = lambda username: None
    module.issue_refresh_token = lambda username: f"rt-{username}"
    module.check_rate_limit = lambda key, limit: None

    register_response = module.register(
        module.AuthIn(username="alice", password="secret123"),
        DummyRequest(),
        Response(),
    )
    assert register_response["username"] == "alice"

    login_response = module.login(
        module.AuthIn(username="alice", password="secret123"),
        DummyRequest(),
        Response(),
    )
    assert login_response["username"] == "alice"

def test_login_invalid_credentials_returns_401(monkeypatch):
    module = _load_main_module(monkeypatch)

    class DummyRequest:
        class Client:
            host = "127.0.0.1"

        client = Client()

    class DummyCursor:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, query, params=None):
            return None

        def fetchone(self):
            return None

    class DummyConn:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def cursor(self):
            return DummyCursor()

    module.db = lambda: DummyConn()
    module.check_rate_limit = lambda key, limit: None

    with pytest.raises(module.HTTPException) as err:
        module.login(
            module.AuthIn(username="alice", password="wrong-pass"),
            DummyRequest(),
            Response(),
        )

    assert err.value.status_code == 401
    assert err.value.detail == "Invalid credentials"



def test_healthcheck_payload(monkeypatch):
    module = _load_main_module(monkeypatch)

    response = module.healthcheck()

    assert response["ok"] is True
    assert isinstance(response["ts"], int)


def test_refresh_rotates_token(monkeypatch):
    module = _load_main_module(monkeypatch)

    now = 1_700_000_000
    store = {
        "old-rt": {
            "username": "alice",
            "session_id": "sess-1",
            "expires_at": now + 3600,
            "revoked": False,
            "replaced_by": None,
            "compromised": False,
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
                token = params[0]
                self._row = store.get(token)
            elif query.startswith("INSERT INTO refresh_tokens"):
                token, username, session_id, _created_at, expires_at = params
                store[token] = {
                    "username": username,
                    "session_id": session_id,
                    "expires_at": expires_at,
                    "revoked": False,
                    "replaced_by": None,
                    "compromised": False,
                }
            elif query.startswith("UPDATE refresh_tokens SET revoked=TRUE, replaced_by="):
                new_rt, old_rt = params
                store[old_rt]["revoked"] = True
                store[old_rt]["replaced_by"] = new_rt
            elif query.startswith("UPDATE refresh_tokens SET revoked=TRUE, compromised=TRUE"):
                username = params[0]
                for token_data in store.values():
                    if token_data["username"] == username and not token_data["revoked"]:
                        token_data["revoked"] = True
                        token_data["compromised"] = True

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

    response = Response()
    payload = module.refresh_tokens(DummyRequest(), response)

    assert payload["username"] == "alice"
    assert store["old-rt"]["revoked"] is True
    assert store["old-rt"]["replaced_by"] == "new-rt"
    assert store["new-rt"]["session_id"] == "sess-1"


def test_refresh_reuse_revokes_active_sessions(monkeypatch):
    module = _load_main_module(monkeypatch)

    now = 1_700_000_000
    store = {
        "old-rt": {
            "username": "alice",
            "session_id": "sess-1",
            "expires_at": now + 3600,
            "revoked": True,
            "replaced_by": "new-rt",
            "compromised": False,
        },
        "new-rt": {
            "username": "alice",
            "session_id": "sess-1",
            "expires_at": now + 3600,
            "revoked": False,
            "replaced_by": None,
            "compromised": False,
        },
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
                token = params[0]
                self._row = store.get(token)
            elif query.startswith("UPDATE refresh_tokens SET revoked=TRUE, compromised=TRUE"):
                username = params[0]
                for token_data in store.values():
                    if token_data["username"] == username and not token_data["revoked"]:
                        token_data["revoked"] = True
                        token_data["compromised"] = True

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

    with pytest.raises(module.HTTPException) as err:
        module.refresh_tokens(DummyRequest(), Response())

    assert err.value.status_code == 401
    assert err.value.detail == "Refresh token reuse detected"
    assert store["new-rt"]["revoked"] is True
    assert store["new-rt"]["compromised"] is True
