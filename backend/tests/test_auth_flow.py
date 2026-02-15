import importlib.util
from pathlib import Path


def _load_main_module(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "postgresql://user:pass@localhost:5432/test")
    monkeypatch.setenv("JWT_SECRET", "test-secret-123456")
    monkeypatch.setenv("JWT_SECRET", "test-secret")
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

    register_response = module.register(module.AuthIn(username="alice", password="secret123"), DummyRequest())
    assert register_response["username"] == "alice"

    login_response = module.login(module.AuthIn(username="alice", password="secret123"), DummyRequest())
    assert login_response["username"] == "alice"



def test_healthcheck_payload(monkeypatch):
    module = _load_main_module(monkeypatch)

    response = module.healthcheck()

    assert response["ok"] is True
    assert isinstance(response["ts"], int)
