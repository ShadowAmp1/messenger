from __future__ import annotations

import os
import time
import json
import sqlite3
import hashlib
import hmac
import secrets
import re
from typing import Dict, Set, Optional

from fastapi import (
    FastAPI,
    WebSocket,
    WebSocketDisconnect,
    HTTPException,
    Request,
    Header,
    Depends,
    UploadFile,
    File,
    Form,
)
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel


# =========================
# Config
# =========================
BASE_DIR = os.path.dirname(__file__)
DB_PATH = os.path.join(BASE_DIR, "app.db")

JWT_SECRET = os.environ.get("JWT_SECRET", "dev-secret-change-me")
JWT_TTL_SECONDS = 60 * 60 * 24 * 7  # 7 days

UPLOAD_DIR = os.path.join(BASE_DIR, "uploads")
MAX_UPLOAD_MB = int(os.environ.get("MAX_UPLOAD_MB", "25"))
MAX_UPLOAD_BYTES = MAX_UPLOAD_MB * 1024 * 1024

ALLOWED_IMAGE_MIME = {"image/jpeg", "image/png", "image/webp", "image/gif"}
ALLOWED_VIDEO_MIME = {"video/mp4", "video/webm", "video/quicktime"}  # mp4/webm/mov


# =========================
# DB
# =========================
def db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def _col_exists(conn: sqlite3.Connection, table: str, col: str) -> bool:
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({table})")
    return any(r[1] == col for r in cur.fetchall())


def init_db() -> None:
    conn = db()
    cur = conn.cursor()

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            pass_hash TEXT NOT NULL,
            created_at INTEGER NOT NULL
        );
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS chats (
            id TEXT PRIMARY KEY,
            type TEXT NOT NULL,
            title TEXT NOT NULL,
            created_by TEXT NOT NULL,
            created_at INTEGER NOT NULL
        );
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS chat_members (
            chat_id TEXT NOT NULL,
            username TEXT NOT NULL,
            joined_at INTEGER NOT NULL,
            PRIMARY KEY(chat_id, username)
        );
        """
    )

    # messages: теперь поддерживаем медиа
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id TEXT NOT NULL,
            sender TEXT NOT NULL,
            text TEXT NOT NULL,
            created_at INTEGER NOT NULL,
            media_kind TEXT,
            media_url TEXT,
            media_mime TEXT,
            media_name TEXT
        );
        """
    )

    # миграция для старой БД (если таблица была без колонок медиа)
    if not _col_exists(conn, "messages", "media_kind"):
        cur.execute("ALTER TABLE messages ADD COLUMN media_kind TEXT")
    if not _col_exists(conn, "messages", "media_url"):
        cur.execute("ALTER TABLE messages ADD COLUMN media_url TEXT")
    if not _col_exists(conn, "messages", "media_mime"):
        cur.execute("ALTER TABLE messages ADD COLUMN media_mime TEXT")
    if not _col_exists(conn, "messages", "media_name"):
        cur.execute("ALTER TABLE messages ADD COLUMN media_name TEXT")

    conn.commit()
    conn.close()


# =========================
# Password hashing
# =========================
def hash_password(password: str, salt: Optional[str] = None) -> str:
    if salt is None:
        salt = secrets.token_hex(16)
    dk = hashlib.pbkdf2_hmac(
        "sha256",
        password.encode("utf-8"),
        salt.encode("utf-8"),
        200_000,
    )
    return f"pbkdf2_sha256$200000${salt}${dk.hex()}"


def verify_password(password: str, stored: str) -> bool:
    try:
        _, _, salt, _ = stored.split("$", 3)
    except ValueError:
        return False
    test = hash_password(password, salt)
    return hmac.compare_digest(test, stored)


# =========================
# Minimal JWT HS256
# =========================
def b64url(data: bytes) -> str:
    import base64
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def b64urldecode(data: str) -> bytes:
    import base64
    pad = "=" * (-len(data) % 4)
    return base64.urlsafe_b64decode((data + pad).encode("ascii"))


def jwt_sign(payload: dict) -> str:
    header = {"alg": "HS256", "typ": "JWT"}
    header_b64 = b64url(json.dumps(header, separators=(",", ":")).encode("utf-8"))
    payload_b64 = b64url(json.dumps(payload, separators=(",", ":")).encode("utf-8"))
    msg = f"{header_b64}.{payload_b64}".encode("ascii")
    sig = hmac.new(JWT_SECRET.encode("utf-8"), msg, hashlib.sha256).digest()
    return f"{header_b64}.{payload_b64}.{b64url(sig)}"


def jwt_verify(token: str) -> dict:
    try:
        header_b64, payload_b64, sig_b64 = token.split(".", 2)
    except ValueError:
        raise HTTPException(status_code=401, detail="Invalid token")

    msg = f"{header_b64}.{payload_b64}".encode("ascii")
    expected = hmac.new(JWT_SECRET.encode("utf-8"), msg, hashlib.sha256).digest()
    if not hmac.compare_digest(b64url(expected), sig_b64):
        raise HTTPException(status_code=401, detail="Bad signature")

    payload = json.loads(b64urldecode(payload_b64))
    if int(payload.get("exp", 0)) < int(time.time()):
        raise HTTPException(status_code=401, detail="Token expired")
    return payload


# =========================
# Auth helpers (Bearer preferred, token query supported)
# =========================
def _extract_bearer(authorization: Optional[str]) -> Optional[str]:
    if not authorization:
        return None
    parts = authorization.split(" ", 1)
    if len(parts) != 2:
        return None
    scheme, value = parts[0].strip(), parts[1].strip()
    if scheme.lower() != "bearer" or not value:
        return None
    return value


def get_token(
    request: Request,
    authorization: Optional[str] = Header(default=None),
) -> str:
    token = _extract_bearer(authorization)
    if token:
        return token

    token_q = (request.query_params.get("token") or "").strip()
    if token_q:
        return token_q

    raise HTTPException(status_code=401, detail="Missing token")


def require_user(token: str) -> str:
    payload = jwt_verify(token)
    return payload["sub"]


def get_current_username(token: str = Depends(get_token)) -> str:
    return require_user(token)


# =========================
# Helpers
# =========================
USERNAME_RE = re.compile(r"^[a-zA-Z0-9_]{3,20}$")


def make_id(prefix: str = "") -> str:
    return prefix + secrets.token_urlsafe(12)


def is_member(conn: sqlite3.Connection, chat_id: str, username: str) -> bool:
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM chat_members WHERE chat_id=? AND username=? LIMIT 1", (chat_id, username))
    return cur.fetchone() is not None


def ensure_default_chat_for(conn: sqlite3.Connection, username: str) -> None:
    cur = conn.cursor()
    cur.execute("SELECT id FROM chats WHERE id='general'")
    row = cur.fetchone()
    if not row:
        cur.execute(
            "INSERT INTO chats(id, type, title, created_by, created_at) VALUES(?,?,?,?,?)",
            ("general", "group", "general", "system", int(time.time())),
        )
    cur.execute(
        "INSERT OR IGNORE INTO chat_members(chat_id, username, joined_at) VALUES(?,?,?)",
        ("general", username, int(time.time())),
    )
    conn.commit()


def safe_ext_from_mime(mime: str) -> str:
    # минимально-ожидаемые расширения
    if mime == "image/jpeg": return ".jpg"
    if mime == "image/png": return ".png"
    if mime == "image/webp": return ".webp"
    if mime == "image/gif": return ".gif"
    if mime == "video/mp4": return ".mp4"
    if mime == "video/webm": return ".webm"
    if mime == "video/quicktime": return ".mov"
    return ""


# =========================
# App
# =========================
init_db()
os.makedirs(UPLOAD_DIR, exist_ok=True)

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

FRONTEND_DIR = os.path.abspath(os.path.join(BASE_DIR, "..", "frontend"))


@app.get("/")
def index():
    return FileResponse(os.path.join(FRONTEND_DIR, "index.html"))


app.mount("/static", StaticFiles(directory=FRONTEND_DIR), name="static")
app.mount("/uploads", StaticFiles(directory=UPLOAD_DIR), name="uploads")


# =========================
# Schemas
# =========================
class AuthIn(BaseModel):
    username: str
    password: str


class ChatCreateIn(BaseModel):
    title: str


class DMCreateIn(BaseModel):
    username: str


# =========================
# Auth
# =========================
@app.post("/api/register")
def register(data: AuthIn):
    username = data.username.strip()
    password = data.password

    if not USERNAME_RE.match(username):
        raise HTTPException(status_code=400, detail="Username: 3-20 символов, только буквы/цифры/_.")

    if len(password) < 6:
        raise HTTPException(status_code=400, detail="Password: минимум 6 символов.")

    conn = db()
    cur = conn.cursor()
    try:
        cur.execute(
            "INSERT INTO users(username, pass_hash, created_at) VALUES(?,?,?)",
            (username, hash_password(password), int(time.time())),
        )
        conn.commit()
        ensure_default_chat_for(conn, username)
    except sqlite3.IntegrityError:
        raise HTTPException(status_code=400, detail="Такой username уже занят.")
    finally:
        conn.close()

    return {"ok": True}


@app.post("/api/login")
def login(data: AuthIn):
    username = data.username.strip()

    conn = db()
    cur = conn.cursor()
    cur.execute("SELECT username, pass_hash FROM users WHERE username = ?", (username,))
    row = cur.fetchone()
    if not row or not verify_password(data.password, row["pass_hash"]):
        conn.close()
        raise HTTPException(status_code=401, detail="Invalid credentials")

    ensure_default_chat_for(conn, username)
    conn.close()

    now = int(time.time())
    token = jwt_sign({"sub": username, "iat": now, "exp": now + JWT_TTL_SECONDS})
    return {"token": token, "username": username}


@app.get("/api/me")
def me(username: str = Depends(get_current_username)):
    return {"username": username}


# =========================
# Chats
# =========================
@app.get("/api/chats")
def list_chats(username: str = Depends(get_current_username)):
    conn = db()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT c.id, c.type, c.title, c.created_at
        FROM chats c
        JOIN chat_members m ON m.chat_id = c.id
        WHERE m.username = ?
        ORDER BY c.created_at DESC
        """,
        (username,),
    )
    rows = cur.fetchall()
    conn.close()
    return {"chats": [dict(r) for r in rows]}


@app.post("/api/chats")
def create_chat(data: ChatCreateIn, username: str = Depends(get_current_username)):
    title = data.title.strip()
    if not title or len(title) > 40:
        raise HTTPException(status_code=400, detail="Название чата: 1-40 символов.")

    chat_id = make_id("c_")
    now = int(time.time())

    conn = db()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO chats(id, type, title, created_by, created_at) VALUES(?,?,?,?,?)",
        (chat_id, "group", title, username, now),
    )
    cur.execute(
        "INSERT INTO chat_members(chat_id, username, joined_at) VALUES(?,?,?)",
        (chat_id, username, now),
    )
    conn.commit()
    conn.close()
    return {"chat": {"id": chat_id, "type": "group", "title": title, "created_at": now}}


@app.post("/api/chats/dm")
def create_dm(data: DMCreateIn, username: str = Depends(get_current_username)):
    other = data.username.strip()
    if not USERNAME_RE.match(other):
        raise HTTPException(status_code=400, detail="Некорректный username.")
    if other == username:
        raise HTTPException(status_code=400, detail="Нельзя создать DM с самим собой.")

    conn = db()
    cur = conn.cursor()

    cur.execute("SELECT 1 FROM users WHERE username=? LIMIT 1", (other,))
    if cur.fetchone() is None:
        conn.close()
        raise HTTPException(status_code=404, detail="Пользователь не найден.")

    a, b = sorted([username, other])
    dm_key = f"dm:{a}|{b}"

    cur.execute("SELECT id, created_at FROM chats WHERE type='dm' AND title=?", (dm_key,))
    row = cur.fetchone()
    if row:
        chat_id = row["id"]
        created_at = row["created_at"]
    else:
        chat_id = make_id("d_")
        created_at = int(time.time())
        cur.execute(
            "INSERT INTO chats(id, type, title, created_by, created_at) VALUES(?,?,?,?,?)",
            (chat_id, "dm", dm_key, username, created_at),
        )

    now = int(time.time())
    cur.execute(
        "INSERT OR IGNORE INTO chat_members(chat_id, username, joined_at) VALUES(?,?,?)",
        (chat_id, username, now),
    )
    cur.execute(
        "INSERT OR IGNORE INTO chat_members(chat_id, username, joined_at) VALUES(?,?,?)",
        (chat_id, other, now),
    )

    conn.commit()
    conn.close()

    return {"chat": {"id": chat_id, "type": "dm", "title": f"DM: {other}", "created_at": created_at}}


# =========================
# Messages
# =========================
@app.get("/api/messages")
def list_messages(chat_id: str, username: str = Depends(get_current_username)):
    conn = db()
    if not is_member(conn, chat_id, username):
        conn.close()
        raise HTTPException(status_code=403, detail="Нет доступа к этому чату.")

    cur = conn.cursor()
    cur.execute(
        """
        SELECT id, chat_id, sender, text, created_at, media_kind, media_url, media_mime, media_name
        FROM messages
        WHERE chat_id=?
        ORDER BY id DESC
        LIMIT 100
        """,
        (chat_id,),
    )
    rows = cur.fetchall()
    conn.close()
    msgs = [dict(r) for r in rows][::-1]
    return {"chat_id": chat_id, "messages": msgs}


# =========================
# Upload media (creates message + broadcast)
# =========================
@app.post("/api/upload")
async def upload_media(
    chat_id: str = Form(...),
    text: str = Form(""),
    file: UploadFile = File(...),
    username: str = Depends(get_current_username),
):
    text = (text or "").strip()
    if len(text) > 2000:
        raise HTTPException(status_code=400, detail="Текст: максимум 2000 символов.")

    conn = db()
    if not is_member(conn, chat_id, username):
        conn.close()
        raise HTTPException(status_code=403, detail="Нет доступа к этому чату.")
    conn.close()

    mime = (file.content_type or "").lower().strip()
    if mime in ALLOWED_IMAGE_MIME:
        kind = "image"
    elif mime in ALLOWED_VIDEO_MIME:
        kind = "video"
    else:
        raise HTTPException(status_code=400, detail=f"Неподдерживаемый тип файла: {mime or 'unknown'}")

    data = await file.read()
    if not data:
        raise HTTPException(status_code=400, detail="Пустой файл.")
    if len(data) > MAX_UPLOAD_BYTES:
        raise HTTPException(status_code=400, detail=f"Слишком большой файл. Лимит: {MAX_UPLOAD_MB} MB")

    ext = safe_ext_from_mime(mime)
    fname = f"{int(time.time())}_{secrets.token_urlsafe(10)}{ext}"
    path = os.path.join(UPLOAD_DIR, fname)

    with open(path, "wb") as f:
        f.write(data)

    url = f"/uploads/{fname}"
    ts = int(time.time())

    conn = db()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO messages(chat_id, sender, text, created_at, media_kind, media_url, media_mime, media_name)
        VALUES(?,?,?,?,?,?,?,?)
        """,
        (
            chat_id,
            username,
            text,
            ts,
            kind,
            url,
            mime,
            (file.filename or "")[:200],
        ),
    )
    conn.commit()
    msg_id = cur.lastrowid
    conn.close()

    event = {
        "type": "message",
        "id": msg_id,
        "chat_id": chat_id,
        "sender": username,
        "text": text,
        "created_at": ts,
        "media_kind": kind,
        "media_url": url,
        "media_mime": mime,
        "media_name": (file.filename or "")[:200],
    }

    await broadcast(chat_id, event)
    return {"message": event}


# =========================
# WebSocket: per chat (text messages)
# =========================
connections: Dict[str, Set[WebSocket]] = {}


def add_conn(chat_id: str, ws: WebSocket) -> None:
    connections.setdefault(chat_id, set()).add(ws)


def remove_conn(chat_id: str, ws: WebSocket) -> None:
    if chat_id in connections:
        connections[chat_id].discard(ws)
        if not connections[chat_id]:
            connections.pop(chat_id, None)


@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):
    token = ws.query_params.get("token") or ""
    chat_id = ws.query_params.get("chat_id") or ""

    if not token or not chat_id:
        await ws.close(code=4401)
        return

    try:
        payload = jwt_verify(token)
        username = payload["sub"]
    except HTTPException:
        await ws.close(code=4401)
        return

    conn = db()
    if not is_member(conn, chat_id, username):
        conn.close()
        await ws.close(code=4403)
        return
    conn.close()

    await ws.accept()
    add_conn(chat_id, ws)

    try:
        while True:
            raw = await ws.receive_text()
            data = json.loads(raw)

            if data.get("type") != "message":
                continue

            text = (data.get("text") or "").strip()
            if not text:
                continue
            if len(text) > 2000:
                continue

            ts = int(time.time())

            conn = db()
            cur = conn.cursor()
            cur.execute(
                """
                INSERT INTO messages(chat_id, sender, text, created_at, media_kind, media_url, media_mime, media_name)
                VALUES(?,?,?,?,NULL,NULL,NULL,NULL)
                """,
                (chat_id, username, text, ts),
            )
            conn.commit()
            msg_id = cur.lastrowid
            conn.close()

            event = {
                "type": "message",
                "id": msg_id,
                "chat_id": chat_id,
                "sender": username,
                "text": text,
                "created_at": ts,
                "media_kind": None,
                "media_url": None,
                "media_mime": None,
                "media_name": None,
            }
            await broadcast(chat_id, event)

    except WebSocketDisconnect:
        pass
    finally:
        remove_conn(chat_id, ws)


async def broadcast(chat_id: str, event: dict) -> None:
    dead = []
    for c in list(connections.get(chat_id, set())):
        try:
            await c.send_text(json.dumps(event))
        except Exception:
            dead.append(c)
    for d in dead:
        remove_conn(chat_id, d)
