from __future__ import annotations

import os
import time
import json
import re
import hmac
import hashlib
import secrets
from typing import Dict, Set, Optional, List, Any, Tuple

import psycopg
from psycopg.rows import dict_row

import cloudinary
import cloudinary.uploader

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
    Query,
)
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel


# =========================
# Paths (MONOREPO)
# backend/main.py
# frontend/index.html
# =========================
BACKEND_DIR = os.path.dirname(os.path.abspath(__file__))     # .../backend
PROJECT_ROOT = os.path.dirname(BACKEND_DIR)                  # .../
FRONTEND_DIR = os.path.join(PROJECT_ROOT, "frontend")        # .../frontend
FRONTEND_INDEX = os.path.join(FRONTEND_DIR, "index.html")


# =========================
# Config
# =========================
JWT_SECRET = os.environ.get("JWT_SECRET", "dev-secret-change-me")
JWT_TTL_SECONDS = int(os.environ.get("JWT_TTL_SECONDS", str(60 * 60 * 24 * 30)))  # 30 days

DATABASE_URL = (os.environ.get("DATABASE_URL") or "").strip()
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL env is required")

# Normalize for psycopg
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = "postgresql://" + DATABASE_URL[len("postgres://"):]

MAX_UPLOAD_MB = int(os.environ.get("MAX_UPLOAD_MB", "25"))
MAX_UPLOAD_BYTES = MAX_UPLOAD_MB * 1024 * 1024

ALLOWED_IMAGE_MIME = {"image/jpeg", "image/png", "image/webp", "image/gif"}
ALLOWED_VIDEO_MIME = {"video/mp4", "video/webm", "video/quicktime"}  # mov
ALLOWED_AUDIO_MIME = {
    "audio/webm",
    "audio/ogg",
    "audio/wav",
    "audio/mpeg",
    "audio/mp4",
    "audio/aac",
    "audio/x-m4a",
    "audio/m4a",
}

USERNAME_RE = re.compile(r"^[a-zA-Z0-9_]{3,20}$")


# =========================
# Cloudinary config
# =========================
CLOUDINARY_CLOUD_NAME = (os.environ.get("CLOUDINARY_CLOUD_NAME") or "").strip()
CLOUDINARY_API_KEY = (os.environ.get("CLOUDINARY_API_KEY") or "").strip()
CLOUDINARY_API_SECRET = (os.environ.get("CLOUDINARY_API_SECRET") or "").strip()

if not (CLOUDINARY_CLOUD_NAME and CLOUDINARY_API_KEY and CLOUDINARY_API_SECRET):
    raise RuntimeError(
        "Cloudinary env vars required: CLOUDINARY_CLOUD_NAME, CLOUDINARY_API_KEY, CLOUDINARY_API_SECRET"
    )

cloudinary.config(
    cloud_name=CLOUDINARY_CLOUD_NAME,
    api_key=CLOUDINARY_API_KEY,
    api_secret=CLOUDINARY_API_SECRET,
    secure=True,
)


# =========================
# DB helpers
# =========================
def db():
    # new connection per action (simple + safe)
    return psycopg.connect(DATABASE_URL, row_factory=dict_row)


def init_db() -> None:
    """
    Safe "migrations" via CREATE + ALTER ... IF NOT EXISTS.
    Render free tier -> keep it simple (no Alembic).
    """
    with db() as conn:
        with conn.cursor() as cur:
            # users
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS users (
                    id BIGSERIAL PRIMARY KEY,
                    username TEXT UNIQUE NOT NULL,
                    pass_hash TEXT NOT NULL,
                    avatar_url TEXT,
                    created_at BIGINT NOT NULL
                );
                """
            )
            # chats
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS chats (
                    id TEXT PRIMARY KEY,
                    type TEXT NOT NULL,         -- 'group' | 'dm'
                    title TEXT NOT NULL,        -- group: title; dm: dm:alice|bob
                    created_by TEXT NOT NULL,
                    created_at BIGINT NOT NULL
                );
                """
            )
            # members
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS chat_members (
                    chat_id TEXT NOT NULL,
                    username TEXT NOT NULL,
                    joined_at BIGINT NOT NULL,
                    PRIMARY KEY(chat_id, username)
                );
                """
            )
            # messages
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS messages (
                    id BIGSERIAL PRIMARY KEY,
                    chat_id TEXT NOT NULL,
                    sender TEXT NOT NULL,
                    text TEXT NOT NULL,
                    created_at BIGINT NOT NULL,
                    updated_at BIGINT,
                    is_edited BOOLEAN DEFAULT FALSE,
                    deleted_for_all BOOLEAN DEFAULT FALSE,
                    media_kind TEXT,
                    media_url TEXT,
                    media_mime TEXT,
                    media_name TEXT
                );
                """
            )
            # read markers
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS chat_reads (
                    chat_id TEXT NOT NULL,
                    username TEXT NOT NULL,
                    last_read_id BIGINT NOT NULL DEFAULT 0,
                    updated_at BIGINT NOT NULL,
                    PRIMARY KEY(chat_id, username)
                );
                """
            )
            # delete for me (hide)
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS message_hidden (
                    message_id BIGINT NOT NULL,
                    username TEXT NOT NULL,
                    hidden_at BIGINT NOT NULL,
                    PRIMARY KEY(message_id, username)
                );
                """
            )
            # delivered
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS message_delivered (
                    message_id BIGINT NOT NULL,
                    username TEXT NOT NULL,
                    delivered_at BIGINT NOT NULL,
                    PRIMARY KEY(message_id, username)
                );
                """
            )

            # --- backfill columns for older DBs ---
            cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS avatar_url TEXT;")

            cur.execute("ALTER TABLE messages ADD COLUMN IF NOT EXISTS updated_at BIGINT;")
            cur.execute("ALTER TABLE messages ADD COLUMN IF NOT EXISTS is_edited BOOLEAN DEFAULT FALSE;")
            cur.execute("ALTER TABLE messages ADD COLUMN IF NOT EXISTS deleted_for_all BOOLEAN DEFAULT FALSE;")
            cur.execute("ALTER TABLE messages ADD COLUMN IF NOT EXISTS media_kind TEXT;")
            cur.execute("ALTER TABLE messages ADD COLUMN IF NOT EXISTS media_url TEXT;")
            cur.execute("ALTER TABLE messages ADD COLUMN IF NOT EXISTS media_mime TEXT;")
            cur.execute("ALTER TABLE messages ADD COLUMN IF NOT EXISTS media_name TEXT;")

        conn.commit()


def is_member(conn, chat_id: str, username: str) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM chat_members WHERE chat_id=%s AND username=%s LIMIT 1",
            (chat_id, username),
        )
        return cur.fetchone() is not None


def ensure_general_for(username: str) -> None:
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM chats WHERE id='general'")
            if cur.fetchone() is None:
                cur.execute(
                    "INSERT INTO chats(id, type, title, created_by, created_at) VALUES(%s,%s,%s,%s,%s)",
                    ("general", "group", "general", "system", int(time.time())),
                )

            cur.execute(
                """
                INSERT INTO chat_members(chat_id, username, joined_at)
                VALUES (%s,%s,%s)
                ON CONFLICT (chat_id, username) DO NOTHING
                """,
                ("general", username, int(time.time())),
            )

            cur.execute(
                """
                INSERT INTO chat_reads(chat_id, username, last_read_id, updated_at)
                VALUES (%s,%s,0,%s)
                ON CONFLICT (chat_id, username) DO NOTHING
                """,
                ("general", username, int(time.time())),
            )
        conn.commit()


def list_members(chat_id: str) -> List[str]:
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT username FROM chat_members WHERE chat_id=%s", (chat_id,))
            return [r["username"] for r in cur.fetchall()]


def get_chat(conn, chat_id: str) -> Optional[dict]:
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM chats WHERE id=%s", (chat_id,))
        return cur.fetchone()


# =========================
# Password hashing (PBKDF2)
# =========================
def hash_password(password: str, salt: Optional[str] = None) -> str:
    if salt is None:
        salt = secrets.token_hex(16)
    dk = hashlib.pbkdf2_hmac("sha256", password.encode(), salt.encode(), 200_000)
    return f"pbkdf2_sha256$200000${salt}${dk.hex()}"


def verify_password(password: str, stored: str) -> bool:
    try:
        _, _, salt, _ = stored.split("$", 3)
    except ValueError:
        return False
    return hmac.compare_digest(hash_password(password, salt), stored)


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
    header_b64 = b64url(json.dumps(header, separators=(",", ":")).encode())
    payload_b64 = b64url(json.dumps(payload, separators=(",", ":")).encode())
    msg = f"{header_b64}.{payload_b64}".encode("ascii")
    sig = hmac.new(JWT_SECRET.encode(), msg, hashlib.sha256).digest()
    return f"{header_b64}.{payload_b64}.{b64url(sig)}"


def jwt_verify(token: str) -> dict:
    try:
        header_b64, payload_b64, sig_b64 = token.split(".", 2)
    except ValueError:
        raise HTTPException(status_code=401, detail="Invalid token")

    msg = f"{header_b64}.{payload_b64}".encode("ascii")
    expected = hmac.new(JWT_SECRET.encode(), msg, hashlib.sha256).digest()
    if not hmac.compare_digest(b64url(expected), sig_b64):
        raise HTTPException(status_code=401, detail="Bad signature")

    payload = json.loads(b64urldecode(payload_b64))
    if int(payload.get("exp", 0)) < int(time.time()):
        raise HTTPException(status_code=401, detail="Token expired")
    return payload


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


def get_token(request: Request, authorization: Optional[str] = Header(default=None)) -> str:
    token = _extract_bearer(authorization)
    if token:
        return token
    token_q = (request.query_params.get("token") or "").strip()
    if token_q:
        return token_q
    raise HTTPException(status_code=401, detail="Missing token")


def get_current_username(token: str = Depends(get_token)) -> str:
    return jwt_verify(token)["sub"]


# =========================
# Misc helpers
# =========================
def make_id(prefix: str = "") -> str:
    return prefix + secrets.token_urlsafe(10)


def media_kind_from_mime(mime: str) -> str:
    mime = (mime or "").lower().strip()
    if mime in ALLOWED_IMAGE_MIME or mime.startswith("image/"):
        return "image"
    if mime in ALLOWED_VIDEO_MIME or mime.startswith("video/"):
        return "video"
    if mime in ALLOWED_AUDIO_MIME or mime.startswith("audio/"):
        return "audio"
    return ""


def cloudinary_resource_type(kind: str) -> str:
    # Cloudinary treats audio as "video" resource in most cases.
    if kind == "image":
        return "image"
    if kind in ("video", "audio"):
        return "video"
    return "raw"


def now_ts() -> int:
    return int(time.time())


# =========================
# Realtime (Global WS per user)
# =========================
USER_SOCKETS: Dict[str, Set[WebSocket]] = {}


def _ws_add(username: str, ws: WebSocket) -> None:
    USER_SOCKETS.setdefault(username, set()).add(ws)


def _ws_remove(username: str, ws: WebSocket) -> None:
    if username in USER_SOCKETS:
        USER_SOCKETS[username].discard(ws)
        if not USER_SOCKETS[username]:
            USER_SOCKETS.pop(username, None)


async def ws_send_safe(ws: WebSocket, payload: dict) -> None:
    try:
        await ws.send_text(json.dumps(payload))
    except Exception:
        # will be cleaned on next disconnect
        pass


async def broadcast_users(usernames: List[str], payload: dict) -> None:
    if not usernames:
        return
    sent_to = set()
    for u in usernames:
        if u in sent_to:
            continue
        sent_to.add(u)
        for ws in list(USER_SOCKETS.get(u, set())):
            await ws_send_safe(ws, payload)


async def broadcast_chat(chat_id: str, payload: dict) -> None:
    await broadcast_users(list_members(chat_id), payload)


# =========================
# App
# =========================
app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
def _startup():
    init_db()


# Serve frontend
if os.path.isdir(FRONTEND_DIR):
    app.mount("/frontend", StaticFiles(directory=FRONTEND_DIR, html=True), name="frontend")


@app.get("/")
def root():
    # Serve index.html from frontend folder if exists, else show basic text.
    if os.path.isfile(FRONTEND_INDEX):
        return FileResponse(FRONTEND_INDEX)
    return {"ok": True, "hint": "frontend/index.html not found"}


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


class InviteIn(BaseModel):
    username: str


class MessageCreateIn(BaseModel):
    chat_id: str
    text: str


class MessageEditIn(BaseModel):
    text: str


# =========================
# Auth API
# =========================
@app.post("/api/register")
def register(data: AuthIn):
    username = data.username.strip()
    password = data.password

    if not USERNAME_RE.match(username):
        raise HTTPException(status_code=400, detail="Username: 3-20 символов, только буквы/цифры/_.")

    if len(password) < 6:
        raise HTTPException(status_code=400, detail="Password: минимум 6 символов.")

    now = now_ts()
    try:
        with db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO users(username, pass_hash, created_at) VALUES(%s,%s,%s)",
                    (username, hash_password(password), now),
                )
            conn.commit()
    except psycopg.errors.UniqueViolation:
        raise HTTPException(status_code=400, detail="Такой username уже занят.")

    ensure_general_for(username)

    token = jwt_sign({"sub": username, "iat": now, "exp": now + JWT_TTL_SECONDS})
    return {"token": token, "username": username}


@app.post("/api/login")
def login(data: AuthIn):
    username = data.username.strip()

    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT pass_hash FROM users WHERE username=%s", (username,))
            row = cur.fetchone()

    if not row or not verify_password(data.password, row["pass_hash"]):
        raise HTTPException(status_code=401, detail="Invalid credentials")

    ensure_general_for(username)

    now = now_ts()
    token = jwt_sign({"sub": username, "iat": now, "exp": now + JWT_TTL_SECONDS})
    return {"token": token, "username": username}


@app.get("/api/me")
def me(username: str = Depends(get_current_username)):
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT username, avatar_url FROM users WHERE username=%s", (username,))
            row = cur.fetchone()
    return {"username": username, "avatar_url": (row["avatar_url"] if row else None)}


# =========================
# Avatar upload
# =========================
@app.post("/api/avatar")
async def upload_avatar(
    file: UploadFile = File(...),
    username: str = Depends(get_current_username),
):
    content_type = (file.content_type or "").lower()
    if content_type not in ALLOWED_IMAGE_MIME and not content_type.startswith("image/"):
        raise HTTPException(status_code=400, detail="Avatar must be an image")

    data = await file.read()
    if len(data) > MAX_UPLOAD_BYTES:
        raise HTTPException(status_code=413, detail=f"File too large (max {MAX_UPLOAD_MB}MB)")

    try:
        up = cloudinary.uploader.upload(
            data,
            folder="messenger/avatars",
            resource_type="image",
            overwrite=True,
            public_id=f"avatar_{username}",
        )
        url = up.get("secure_url") or up.get("url")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Cloudinary upload failed: {e}")

    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE users SET avatar_url=%s WHERE username=%s", (url, username))
        conn.commit()

    return {"ok": True, "avatar_url": url}


# =========================
# Chats API
# =========================
@app.get("/api/chats")
def list_chats(username: str = Depends(get_current_username)):
    """
    Returns chats with:
      id,type,title,created_by,created_at, unread
    """
    with db() as conn:
        with conn.cursor() as cur:
            # unread = count of messages from others with id > last_read_id,
            # excluding deleted_for_all, and excluding hidden-for-me.
            cur.execute(
                """
                WITH my_chats AS (
                    SELECT c.id, c.type, c.title, c.created_by, c.created_at
                    FROM chats c
                    JOIN chat_members m ON m.chat_id = c.id
                    WHERE m.username=%s
                ),
                reads AS (
                    SELECT chat_id, last_read_id
                    FROM chat_reads
                    WHERE username=%s
                )
                SELECT
                    mc.id, mc.type, mc.title, mc.created_by, mc.created_at,
                    COALESCE((
                        SELECT COUNT(*)
                        FROM messages msg
                        LEFT JOIN message_hidden hid
                          ON hid.message_id = msg.id AND hid.username = %s
                        WHERE msg.chat_id = mc.id
                          AND msg.sender <> %s
                          AND msg.deleted_for_all = FALSE
                          AND hid.message_id IS NULL
                          AND msg.id > COALESCE((SELECT r.last_read_id FROM reads r WHERE r.chat_id = mc.id), 0)
                    ),0) AS unread
                FROM my_chats mc
                ORDER BY mc.created_at DESC
                """,
                (username, username, username, username),
            )
            rows = cur.fetchall()
    return {"chats": rows}


@app.post("/api/chats")
def create_group_chat(data: ChatCreateIn, username: str = Depends(get_current_username)):
    title = data.title.strip()
    if not title or len(title) > 40:
        raise HTTPException(status_code=400, detail="Название чата: 1-40 символов.")

    chat_id = make_id("c_")
    now = now_ts()

    with db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO chats(id, type, title, created_by, created_at) VALUES(%s,%s,%s,%s,%s)",
                (chat_id, "group", title, username, now),
            )
            cur.execute(
                """
                INSERT INTO chat_members(chat_id, username, joined_at)
                VALUES (%s,%s,%s)
                """,
                (chat_id, username, now),
            )
            cur.execute(
                """
                INSERT INTO chat_reads(chat_id, username, last_read_id, updated_at)
                VALUES (%s,%s,0,%s)
                ON CONFLICT (chat_id, username) DO NOTHING
                """,
                (chat_id, username, now),
            )
        conn.commit()

    return {"chat": {"id": chat_id, "title": title}}


def _dm_key(a: str, b: str) -> Tuple[str, str, str]:
    x, y = sorted([a, b])
    title = f"dm:{x}|{y}"
    # deterministic id so DM is unique
    h = hashlib.sha256(title.encode()).hexdigest()[:16]
    chat_id = f"dm_{h}"
    return chat_id, title, y if x == a else x


@app.post("/api/chats/dm")
def create_dm_chat(data: DMCreateIn, username: str = Depends(get_current_username)):
    other = data.username.strip()
    if not USERNAME_RE.match(other):
        raise HTTPException(status_code=400, detail="Bad username")
    if other == username:
        raise HTTPException(status_code=400, detail="Нельзя создать DM с самим собой")

    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM users WHERE username=%s", (other,))
            if cur.fetchone() is None:
                raise HTTPException(status_code=404, detail="User not found")

    chat_id, title, other_name = _dm_key(username, other)
    now = now_ts()

    with db() as conn:
        with conn.cursor() as cur:
            # create chat if not exists
            cur.execute(
                """
                INSERT INTO chats(id, type, title, created_by, created_at)
                VALUES (%s,'dm',%s,%s,%s)
                ON CONFLICT (id) DO NOTHING
                """,
                (chat_id, title, username, now),
            )
            # add members
            for u in {username, other}:
                cur.execute(
                    """
                    INSERT INTO chat_members(chat_id, username, joined_at)
                    VALUES (%s,%s,%s)
                    ON CONFLICT (chat_id, username) DO NOTHING
                    """,
                    (chat_id, u, now),
                )
                cur.execute(
                    """
                    INSERT INTO chat_reads(chat_id, username, last_read_id, updated_at)
                    VALUES (%s,%s,0,%s)
                    ON CONFLICT (chat_id, username) DO NOTHING
                    """,
                    (chat_id, u, now),
                )
        conn.commit()

    return {"chat": {"id": chat_id, "title": f"DM: {other_name}"}}


@app.post("/api/chats/{chat_id}/invite")
async def invite_to_group(
    chat_id: str,
    data: InviteIn,
    username: str = Depends(get_current_username),
):
    other = data.username.strip()
    if not USERNAME_RE.match(other):
        raise HTTPException(status_code=400, detail="Bad username")
    if other == username:
        raise HTTPException(status_code=400, detail="Нельзя пригласить самого себя")

    with db() as conn:
        chat = get_chat(conn, chat_id)
        if not chat:
            raise HTTPException(status_code=404, detail="Chat not found")
        if chat["type"] != "group":
            raise HTTPException(status_code=400, detail="Invite only in group chats")
        if chat["created_by"] != username:
            raise HTTPException(status_code=403, detail="Only creator can invite")

        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM users WHERE username=%s", (other,))
            if cur.fetchone() is None:
                raise HTTPException(status_code=404, detail="User not found")

            now = now_ts()
            cur.execute(
                """
                INSERT INTO chat_members(chat_id, username, joined_at)
                VALUES (%s,%s,%s)
                ON CONFLICT (chat_id, username) DO NOTHING
                """,
                (chat_id, other, now),
            )
            cur.execute(
                """
                INSERT INTO chat_reads(chat_id, username, last_read_id, updated_at)
                VALUES (%s,%s,0,%s)
                ON CONFLICT (chat_id, username) DO NOTHING
                """,
                (chat_id, other, now),
            )
        conn.commit()

    # notify invited user (they will refresh chats)
    await broadcast_users([other], {"type": "invited", "chat_id": chat_id})
    return {"ok": True}


@app.delete("/api/chats/{chat_id}")
async def delete_chat(chat_id: str, username: str = Depends(get_current_username)):
    if chat_id == "general":
        raise HTTPException(status_code=400, detail="general cannot be deleted")

    with db() as conn:
        chat = get_chat(conn, chat_id)
        if not chat:
            raise HTTPException(status_code=404, detail="Chat not found")

        if not is_member(conn, chat_id, username):
            raise HTTPException(status_code=403, detail="Not a member")

        with conn.cursor() as cur:
            if chat["type"] == "group":
                if chat["created_by"] != username:
                    raise HTTPException(status_code=403, detail="Only creator can delete group")
                # delete everything
                cur.execute("DELETE FROM message_hidden WHERE message_id IN (SELECT id FROM messages WHERE chat_id=%s)", (chat_id,))
                cur.execute("DELETE FROM message_delivered WHERE message_id IN (SELECT id FROM messages WHERE chat_id=%s)", (chat_id,))
                cur.execute("DELETE FROM chat_reads WHERE chat_id=%s", (chat_id,))
                cur.execute("DELETE FROM messages WHERE chat_id=%s", (chat_id,))
                cur.execute("DELETE FROM chat_members WHERE chat_id=%s", (chat_id,))
                cur.execute("DELETE FROM chats WHERE id=%s", (chat_id,))
                conn.commit()

                await broadcast_chat(chat_id, {"type": "chat_deleted", "chat_id": chat_id})
                return {"ok": True}

            # dm: remove membership for current user (soft-delete for user)
            cur.execute("DELETE FROM chat_members WHERE chat_id=%s AND username=%s", (chat_id, username))
            cur.execute("DELETE FROM chat_reads WHERE chat_id=%s AND username=%s", (chat_id, username))
            conn.commit()

            # if no members left -> fully delete
            with conn.cursor() as cur2:
                cur2.execute("SELECT COUNT(*) AS n FROM chat_members WHERE chat_id=%s", (chat_id,))
                n = int(cur2.fetchone()["n"])
                if n == 0:
                    cur2.execute("DELETE FROM message_hidden WHERE message_id IN (SELECT id FROM messages WHERE chat_id=%s)", (chat_id,))
                    cur2.execute("DELETE FROM message_delivered WHERE message_id IN (SELECT id FROM messages WHERE chat_id=%s)", (chat_id,))
                    cur2.execute("DELETE FROM chat_reads WHERE chat_id=%s", (chat_id,))
                    cur2.execute("DELETE FROM messages WHERE chat_id=%s", (chat_id,))
                    cur2.execute("DELETE FROM chats WHERE id=%s", (chat_id,))
                    conn.commit()

    # notify remaining member(s) to refresh
    await broadcast_chat(chat_id, {"type": "chat_deleted", "chat_id": chat_id})
    return {"ok": True}


# =========================
# WebSocket: global user channel
# =========================
@app.websocket("/ws/user")
async def ws_user(ws: WebSocket):
    """
    Client connects with ?token=...
    Receives:
      - message
      - message_edited
      - message_deleted_all
      - chat_deleted
      - typing
      - delivered
      - read
      - invited
    Sends:
      - typing {chat_id,is_typing}
      - delivered {chat_id,message_id}
    """
    token = (ws.query_params.get("token") or "").strip()
    if not token:
        await ws.close(code=4401)
        return

    try:
        username = jwt_verify(token)["sub"]
    except HTTPException:
        await ws.close(code=4401)
        return

    await ws.accept()
    _ws_add(username, ws)

    try:
        while True:
            raw = await ws.receive_text()
            try:
                data = json.loads(raw)
            except Exception:
                continue

            t = data.get("type")
            if t == "typing":
                chat_id = (data.get("chat_id") or "").strip()
                is_typing = bool(data.get("is_typing"))
                if not chat_id:
                    continue
                with db() as conn:
                    if not is_member(conn, chat_id, username):
                        continue
                await broadcast_chat(chat_id, {
                    "type": "typing",
                    "chat_id": chat_id,
                    "username": username,
                    "is_typing": is_typing,
                })

            elif t == "delivered":
                chat_id = (data.get("chat_id") or "").strip()
                mid = int(data.get("message_id") or 0)
                if not chat_id or not mid:
                    continue
                with db() as conn:
                    if not is_member(conn, chat_id, username):
                        continue
                    with conn.cursor() as cur:
                        cur.execute(
                            """
                            INSERT INTO message_delivered(message_id, username, delivered_at)
                            VALUES (%s,%s,%s)
                            ON CONFLICT (message_id, username) DO NOTHING
                            """,
                            (mid, username, now_ts()),
                        )
                    conn.commit()

                # rebroadcast so sender can update ✓✓
                await broadcast_chat(chat_id, {
                    "type": "delivered",
                    "chat_id": chat_id,
                    "message_id": mid,
                    "username": username,
                })
    except WebSocketDisconnect:
        pass
    finally:
        _ws_remove(username, ws)


# =========================
# Messages API
# =========================
@app.get("/api/messages")
def list_messages(
    chat_id: str = Query(...),
    username: str = Depends(get_current_username),
):
    chat_id = (chat_id or "").strip()
    if not chat_id:
        raise HTTPException(status_code=400, detail="chat_id required")

    with db() as conn:
        if not is_member(conn, chat_id, username):
            raise HTTPException(status_code=403, detail="Not a member")

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                  m.id, m.chat_id, m.sender, m.text, m.created_at,
                  m.updated_at, m.is_edited, m.deleted_for_all,
                  m.media_kind, m.media_url, m.media_mime, m.media_name
                FROM messages m
                LEFT JOIN message_hidden hid
                  ON hid.message_id = m.id AND hid.username = %s
                WHERE m.chat_id = %s
                  AND hid.message_id IS NULL
                ORDER BY m.id ASC
                LIMIT 500
                """,
                (username, chat_id),
            )
            rows = cur.fetchall()

    return {"messages": rows}


@app.post("/api/messages")
async def create_text_message(
    data: MessageCreateIn,
    username: str = Depends(get_current_username),
):
    chat_id = (data.chat_id or "").strip()
    text = (data.text or "").strip()

    if not chat_id:
        raise HTTPException(status_code=400, detail="chat_id required")
    if not text:
        raise HTTPException(status_code=400, detail="text required")
    if len(text) > 2000:
        raise HTTPException(status_code=400, detail="text too long (max 2000)")

    ts = now_ts()

    with db() as conn:
        if not is_member(conn, chat_id, username):
            raise HTTPException(status_code=403, detail="Not a member")

        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO messages(chat_id, sender, text, created_at)
                VALUES (%s,%s,%s,%s)
                RETURNING id
                """,
                (chat_id, username, text, ts),
            )
            msg_id = int(cur.fetchone()["id"])

            # sender delivered to self (for completeness)
            cur.execute(
                """
                INSERT INTO message_delivered(message_id, username, delivered_at)
                VALUES (%s,%s,%s)
                ON CONFLICT DO NOTHING
                """,
                (msg_id, username, ts),
            )

        conn.commit()

    payload = {
        "type": "message",
        "id": msg_id,
        "chat_id": chat_id,
        "sender": username,
        "text": text,
        "created_at": ts,
        "is_edited": False,
        "deleted_for_all": False,
        "media_kind": None,
        "media_url": None,
        "media_mime": None,
        "media_name": None,
    }
    await broadcast_chat(chat_id, payload)
    return {"ok": True, "id": msg_id}


@app.patch("/api/messages/{message_id}")
async def edit_message(
    message_id: int,
    data: MessageEditIn,
    username: str = Depends(get_current_username),
):
    new_text = (data.text or "").strip()
    if not new_text:
        raise HTTPException(status_code=400, detail="text required")
    if len(new_text) > 2000:
        raise HTTPException(status_code=400, detail="text too long (max 2000)")

    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT chat_id, sender, deleted_for_all FROM messages WHERE id=%s", (message_id,))
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Message not found")

            chat_id = row["chat_id"]
            if not is_member(conn, chat_id, username):
                raise HTTPException(status_code=403, detail="Not a member")
            if row["sender"] != username:
                raise HTTPException(status_code=403, detail="Only sender can edit")
            if row["deleted_for_all"]:
                raise HTTPException(status_code=400, detail="Message deleted")

            cur.execute(
                """
                UPDATE messages
                SET text=%s, is_edited=TRUE, updated_at=%s
                WHERE id=%s
                """,
                (new_text, now_ts(), message_id),
            )
        conn.commit()

    await broadcast_chat(chat_id, {
        "type": "message_edited",
        "chat_id": chat_id,
        "id": message_id,
        "text": new_text,
    })
    return {"ok": True}


@app.delete("/api/messages/{message_id}")
async def delete_message(
    message_id: int,
    scope: str = Query("me"),  # me | all
    username: str = Depends(get_current_username),
):
    scope = (scope or "me").lower().strip()
    if scope not in ("me", "all"):
        raise HTTPException(status_code=400, detail="scope must be me|all")

    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT chat_id, sender FROM messages WHERE id=%s", (message_id,))
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Message not found")
            chat_id = row["chat_id"]

            if not is_member(conn, chat_id, username):
                raise HTTPException(status_code=403, detail="Not a member")

            if scope == "me":
                cur.execute(
                    """
                    INSERT INTO message_hidden(message_id, username, hidden_at)
                    VALUES (%s,%s,%s)
                    ON CONFLICT DO NOTHING
                    """,
                    (message_id, username, now_ts()),
                )
                conn.commit()
                return {"ok": True}

            # scope == all
            if row["sender"] != username:
                raise HTTPException(status_code=403, detail="Only sender can delete for all")

            cur.execute(
                "UPDATE messages SET deleted_for_all=TRUE, updated_at=%s WHERE id=%s",
                (now_ts(), message_id),
            )
        conn.commit()

    await broadcast_chat(chat_id, {
        "type": "message_deleted_all",
        "chat_id": chat_id,
        "id": message_id,
    })
    return {"ok": True}


# =========================
# Upload media (image/video/audio)
# =========================
@app.post("/api/upload")
async def upload_media(
    chat_id: str = Form(...),
    text: str = Form(""),
    file: UploadFile = File(...),
    username: str = Depends(get_current_username),
):
    chat_id = (chat_id or "").strip()
    caption = (text or "").strip()

    if not chat_id:
        raise HTTPException(status_code=400, detail="chat_id required")

    if len(caption) > 2000:
        raise HTTPException(status_code=400, detail="text too long (max 2000)")

    content_type = (file.content_type or "").lower().strip()
    kind = media_kind_from_mime(content_type)
    if not kind:
        raise HTTPException(status_code=400, detail=f"Unsupported file type: {content_type}")

    data = await file.read()
    if len(data) > MAX_UPLOAD_BYTES:
        raise HTTPException(status_code=413, detail=f"File too large (max {MAX_UPLOAD_MB}MB)")

    with db() as conn:
        if not is_member(conn, chat_id, username):
            raise HTTPException(status_code=403, detail="Not a member")

    # upload to Cloudinary
    try:
        res = cloudinary.uploader.upload(
            data,
            folder="messenger/uploads",
            resource_type=cloudinary_resource_type(kind),
            use_filename=True,
            unique_filename=True,
        )
        url = res.get("secure_url") or res.get("url")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Cloudinary upload failed: {e}")

    ts = now_ts()
    media_name = (file.filename or "").strip()[:120]

    with db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO messages(chat_id, sender, text, created_at, media_kind, media_url, media_mime, media_name)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                RETURNING id
                """,
                (chat_id, username, caption, ts, kind, url, content_type, media_name),
            )
            msg_id = int(cur.fetchone()["id"])

            cur.execute(
                """
                INSERT INTO message_delivered(message_id, username, delivered_at)
                VALUES (%s,%s,%s)
                ON CONFLICT DO NOTHING
                """,
                (msg_id, username, ts),
            )
        conn.commit()

    payload = {
        "type": "message",
        "id": msg_id,
        "chat_id": chat_id,
        "sender": username,
        "text": caption,
        "created_at": ts,
        "is_edited": False,
        "deleted_for_all": False,
        "media_kind": kind,
        "media_url": url,
        "media_mime": content_type,
        "media_name": media_name,
    }
    await broadcast_chat(chat_id, payload)
    return {"ok": True, "id": msg_id, "media_url": url, "media_kind": kind}


# =========================
# Read markers (✓✓ read)
# =========================
@app.post("/api/chats/{chat_id}/read")
async def mark_read(
    chat_id: str,
    last_id: int = Query(...),
    username: str = Depends(get_current_username),
):
    chat_id = (chat_id or "").strip()
    last_id = int(last_id or 0)
    if not chat_id or last_id <= 0:
        raise HTTPException(status_code=400, detail="chat_id + last_id required")

    with db() as conn:
        if not is_member(conn, chat_id, username):
            raise HTTPException(status_code=403, detail="Not a member")
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO chat_reads(chat_id, username, last_read_id, updated_at)
                VALUES (%s,%s,%s,%s)
                ON CONFLICT (chat_id, username)
                DO UPDATE SET last_read_id = GREATEST(chat_reads.last_read_id, EXCLUDED.last_read_id),
                              updated_at = EXCLUDED.updated_at
                """,
                (chat_id, username, last_id, now_ts()),
            )
        conn.commit()

    await broadcast_chat(chat_id, {
        "type": "read",
        "chat_id": chat_id,
        "username": username,
        "last_read_id": last_id,
    })
    return {"ok": True}


# =========================
# Health
# =========================
@app.get("/api/health")
def health():
    return {"ok": True}
