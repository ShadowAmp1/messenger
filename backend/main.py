from __future__ import annotations

import os
import time
import json
import re
import hmac
import hashlib
import secrets
from contextlib import asynccontextmanager
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
JWT_SECRET = (os.environ.get("JWT_SECRET") or "").strip()
if not JWT_SECRET:
    JWT_SECRET = secrets.token_urlsafe(48)
    print(
        "[WARN] JWT_SECRET env is missing. Generated an ephemeral secret for this process; "
        "tokens will be invalidated after restart. Set JWT_SECRET in environment for stable auth."
    )
if len(JWT_SECRET) < 16:
    raise RuntimeError("JWT_SECRET must be at least 16 characters")
JWT_TTL_SECONDS = int(os.environ.get("JWT_TTL_SECONDS", str(60 * 60 * 24 * 30)))  # 30 days
REFRESH_TTL_SECONDS = int(os.environ.get("REFRESH_TTL_SECONDS", str(60 * 60 * 24 * 120)))  # 120 days

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
RATE_LIMIT_WINDOW_SECONDS = int(os.environ.get("RATE_LIMIT_WINDOW_SECONDS", "60"))
RATE_LIMIT_MAX_AUTH = int(os.environ.get("RATE_LIMIT_MAX_AUTH", "20"))
RATE_LIMIT_MAX_SEND = int(os.environ.get("RATE_LIMIT_MAX_SEND", "100"))

def parse_cors_origins(value: Optional[str]) -> List[str]:
    if value is None or not value.strip():
        return ["http://localhost"]

    origins = [origin.strip() for origin in value.split(",") if origin.strip()]
    if not origins:
        return ["http://localhost"]

    # Keep order while removing accidental duplicates from CSV input.
    return list(dict.fromkeys(origins))


CORS_ORIGINS = parse_cors_origins(os.environ.get("CORS_ORIGINS"))
cors_origins_env = os.environ.get("CORS_ORIGINS")
if cors_origins_env is None or not cors_origins_env.strip():
    cors_origins_env = "http://localhost"

CORS_ORIGINS = [origin.strip() for origin in cors_origins_env.split(",") if origin.strip()]
if not CORS_ORIGINS:
    CORS_ORIGINS = ["http://localhost"]
CORS_ORIGINS = [
    origin.strip()
    for origin in (os.environ.get("CORS_ORIGINS", "http://localhost") or "http://localhost").split(",")
    if origin.strip()
]

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
                    role TEXT NOT NULL DEFAULT 'member',
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
                    media_name TEXT,
                    reply_to_id BIGINT
                );
                """
            )

            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS message_reactions (
                    message_id BIGINT NOT NULL,
                    username TEXT NOT NULL,
                    emoji TEXT NOT NULL,
                    created_at BIGINT NOT NULL,
                    PRIMARY KEY(message_id, username, emoji)
                );
                """
            )

            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS chat_pins (
                    chat_id TEXT NOT NULL,
                    message_id BIGINT NOT NULL,
                    pinned_by TEXT NOT NULL,
                    pinned_at BIGINT NOT NULL,
                    PRIMARY KEY(chat_id, message_id)
                );
                """
            )

            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS chat_member_settings (
                    chat_id TEXT NOT NULL,
                    username TEXT NOT NULL,
                    muted_until BIGINT,
                    PRIMARY KEY(chat_id, username)
                );
                """
            )

            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS refresh_tokens (
                    token TEXT PRIMARY KEY,
                    username TEXT NOT NULL,
                    created_at BIGINT NOT NULL,
                    expires_at BIGINT NOT NULL,
                    revoked BOOLEAN NOT NULL DEFAULT FALSE
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
            cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS display_name TEXT;")
            cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS bio TEXT;")

            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS user_avatar_history (
                    id BIGSERIAL PRIMARY KEY,
                    username TEXT NOT NULL,
                    avatar_url TEXT NOT NULL,
                    created_at BIGINT NOT NULL
                );
                """
            )

            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS contacts (
                    owner_username TEXT NOT NULL,
                    contact_username TEXT NOT NULL,
                    created_at BIGINT NOT NULL,
                    PRIMARY KEY(owner_username, contact_username)
                );
                """
            )

            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS stories (
                    id BIGSERIAL PRIMARY KEY,
                    username TEXT NOT NULL,
                    media_url TEXT NOT NULL,
                    media_kind TEXT NOT NULL,
                    caption TEXT,
                    created_at BIGINT NOT NULL,
                    expires_at BIGINT NOT NULL
                );
                """
            )

            cur.execute("ALTER TABLE messages ADD COLUMN IF NOT EXISTS updated_at BIGINT;")
            cur.execute("ALTER TABLE messages ADD COLUMN IF NOT EXISTS is_edited BOOLEAN DEFAULT FALSE;")
            cur.execute("ALTER TABLE messages ADD COLUMN IF NOT EXISTS deleted_for_all BOOLEAN DEFAULT FALSE;")
            cur.execute("ALTER TABLE messages ADD COLUMN IF NOT EXISTS media_kind TEXT;")
            cur.execute("ALTER TABLE messages ADD COLUMN IF NOT EXISTS media_url TEXT;")
            cur.execute("ALTER TABLE messages ADD COLUMN IF NOT EXISTS media_mime TEXT;")
            cur.execute("ALTER TABLE messages ADD COLUMN IF NOT EXISTS media_name TEXT;")
            cur.execute("ALTER TABLE messages ADD COLUMN IF NOT EXISTS reply_to_id BIGINT;")
            cur.execute("ALTER TABLE chat_members ADD COLUMN IF NOT EXISTS role TEXT NOT NULL DEFAULT 'member';")

            # Remove legacy auto-created public room "general".
            cur.execute("DELETE FROM message_hidden WHERE message_id IN (SELECT id FROM messages WHERE chat_id='general')")
            cur.execute("DELETE FROM message_delivered WHERE message_id IN (SELECT id FROM messages WHERE chat_id='general')")
            cur.execute("DELETE FROM chat_reads WHERE chat_id='general'")
            cur.execute("DELETE FROM chat_members WHERE chat_id='general'")
            cur.execute("DELETE FROM messages WHERE chat_id='general'")
            cur.execute("DELETE FROM chats WHERE id='general'")

            # Ensure each user has a personal chat "Избранное".
            cur.execute("SELECT username FROM users")
            users = [r["username"] for r in cur.fetchall()]
            ts = int(time.time())
            for uname in users:
                fav_id = f"fav:{uname}"
                cur.execute(
                    """
                    INSERT INTO chats(id, type, title, created_by, created_at)
                    VALUES (%s,%s,%s,%s,%s)
                    ON CONFLICT (id) DO NOTHING
                    """,
                    (fav_id, "dm", "Избранное", uname, ts),
                )
                cur.execute(
                    """
                    INSERT INTO chat_members(chat_id, username, role, joined_at)
                    VALUES (%s,%s,%s,%s)
                    ON CONFLICT (chat_id, username) DO NOTHING
                    """,
                    (fav_id, uname, "owner", ts),
                )
                cur.execute(
                    """
                    INSERT INTO chat_reads(chat_id, username, last_read_id, updated_at)
                    VALUES (%s,%s,%s,%s)
                    ON CONFLICT (chat_id, username) DO NOTHING
                    """,
                    (fav_id, uname, 0, ts),
                )

        conn.commit()


def is_member(conn, chat_id: str, username: str) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM chat_members WHERE chat_id=%s AND username=%s LIMIT 1",
            (chat_id, username),
        )
        return cur.fetchone() is not None


def favorites_chat_id(username: str) -> str:
    return f"fav:{username}"


def ensure_favorites_for(username: str) -> None:
    chat_id = favorites_chat_id(username)
    ts = int(time.time())
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO chats(id, type, title, created_by, created_at)
                VALUES (%s,%s,%s,%s,%s)
                ON CONFLICT (id) DO NOTHING
                """,
                (chat_id, "dm", "Избранное", username, ts),
            )

            cur.execute(
                """
                INSERT INTO chat_members(chat_id, username, role, joined_at)
                VALUES (%s,%s,%s,%s)
                ON CONFLICT (chat_id, username) DO NOTHING
                """,
                (chat_id, username, "owner", ts),
            )

            cur.execute(
                """
                INSERT INTO chat_reads(chat_id, username, last_read_id, updated_at)
                VALUES (%s,%s,%s,%s)
                ON CONFLICT (chat_id, username) DO NOTHING
                """,
                (chat_id, username, 0, ts),
            )
        conn.commit()


def list_members(chat_id: str) -> List[str]:
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT username FROM chat_members WHERE chat_id=%s", (chat_id,))
            return [r["username"] for r in cur.fetchall()]


def get_member_role(conn, chat_id: str, username: str) -> Optional[str]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT role FROM chat_members WHERE chat_id=%s AND username=%s",
            (chat_id, username),
        )
        row = cur.fetchone()
        return (row["role"] if row else None)


def can_moderate(conn, chat_id: str, username: str) -> bool:
    chat = get_chat(conn, chat_id)
    if not chat:
        return False
    if chat["type"] == "dm":
        # In 1:1 chats both participants have equal admin privileges.
        return is_member(conn, chat_id, username)
    role = get_member_role(conn, chat_id, username)
    return role in ("owner", "admin")


def issue_refresh_token(username: str) -> str:
    token = secrets.token_urlsafe(36)
    now = now_ts()
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO refresh_tokens(token, username, created_at, expires_at, revoked) VALUES (%s,%s,%s,%s,FALSE)",
                (token, username, now, now + REFRESH_TTL_SECONDS),
            )
        conn.commit()
    return token


RATE_BUCKETS: Dict[str, List[int]] = {}


def check_rate_limit(bucket: str, limit: int) -> None:
    now = now_ts()
    start = now - RATE_LIMIT_WINDOW_SECONDS
    arr = [t for t in RATE_BUCKETS.get(bucket, []) if t >= start]
    if len(arr) >= limit:
        raise HTTPException(status_code=429, detail="Too many requests. Try again later.")
    arr.append(now)
    RATE_BUCKETS[bucket] = arr


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
@asynccontextmanager
async def _lifespan(app: FastAPI):
    init_db()
    yield


app = FastAPI(lifespan=_lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ORIGINS,
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Serve frontend
if os.path.isdir(FRONTEND_DIR):
    app.mount("/frontend", StaticFiles(directory=FRONTEND_DIR, html=True), name="frontend")


@app.get("/")
def root():
    # Serve index.html from frontend folder if exists, else show basic text.
    if os.path.isfile(FRONTEND_INDEX):
        return FileResponse(FRONTEND_INDEX)
    return {"ok": True, "hint": "frontend/index.html not found"}


@app.get("/app")
def mobile_app_entry():
    if os.path.isfile(FRONTEND_INDEX):
        return FileResponse(FRONTEND_INDEX)
    return {"ok": True, "hint": "frontend/index.html not found"}


@app.get("/api/health")
def healthcheck():
    return {"ok": True, "ts": now_ts()}


@app.get("/sw.js")
def service_worker():
    sw_path = os.path.join(FRONTEND_DIR, "sw.js")
    if os.path.isfile(sw_path):
        return FileResponse(sw_path, media_type="application/javascript")
    raise HTTPException(status_code=404, detail="sw.js not found")


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
    reply_to_id: Optional[int] = None


class MessageEditIn(BaseModel):
    text: str


class RefreshIn(BaseModel):
    refresh_token: str


class ReactionIn(BaseModel):
    emoji: str


class ForwardIn(BaseModel):
    target_chat_id: str


class PinIn(BaseModel):
    message_id: int


class MuteIn(BaseModel):
    muted_minutes: int = 0


class RoleUpdateIn(BaseModel):
    username: str
    role: str


class ProfileUpdateIn(BaseModel):
    display_name: str = ""
    bio: str = ""


class ContactCreateIn(BaseModel):
    username: str


# =========================
# Auth API
# =========================
@app.post("/api/register")
def register(data: AuthIn, request: Request):
    check_rate_limit(f"auth:register:{request.client.host if request.client else 'na'}", RATE_LIMIT_MAX_AUTH)
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

    ensure_favorites_for(username)

    token = jwt_sign({"sub": username, "iat": now, "exp": now + JWT_TTL_SECONDS})
    refresh_token = issue_refresh_token(username)
    return {"token": token, "refresh_token": refresh_token, "username": username}


@app.post("/api/login")
def login(data: AuthIn, request: Request):
    check_rate_limit(f"auth:login:{request.client.host if request.client else 'na'}", RATE_LIMIT_MAX_AUTH)
    username = data.username.strip()

    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT pass_hash FROM users WHERE username=%s", (username,))
            row = cur.fetchone()

    if not row or not verify_password(data.password, row["pass_hash"]):
        raise HTTPException(status_code=401, detail="Invalid credentials")

    ensure_favorites_for(username)

    now = now_ts()
    token = jwt_sign({"sub": username, "iat": now, "exp": now + JWT_TTL_SECONDS})
    refresh_token = issue_refresh_token(username)
    return {"token": token, "refresh_token": refresh_token, "username": username}




@app.post("/api/refresh")
def refresh_tokens(data: RefreshIn):
    rt = (data.refresh_token or "").strip()
    if not rt:
        raise HTTPException(status_code=400, detail="refresh_token required")

    with db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT username, expires_at, revoked FROM refresh_tokens WHERE token=%s",
                (rt,),
            )
            row = cur.fetchone()
            if not row or row["revoked"] or int(row["expires_at"]) < now_ts():
                raise HTTPException(status_code=401, detail="Invalid refresh token")
            username = row["username"]
            cur.execute("UPDATE refresh_tokens SET revoked=TRUE WHERE token=%s", (rt,))
        conn.commit()

    now = now_ts()
    token = jwt_sign({"sub": username, "iat": now, "exp": now + JWT_TTL_SECONDS})
    new_rt = issue_refresh_token(username)
    return {"token": token, "refresh_token": new_rt, "username": username}


@app.get("/api/me")
def me(username: str = Depends(get_current_username)):
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT username, avatar_url, display_name, bio FROM users WHERE username=%s", (username,))
            row = cur.fetchone()
    return {
        "username": username,
        "avatar_url": (row["avatar_url"] if row else None),
        "display_name": (row["display_name"] if row else None),
        "bio": (row["bio"] if row else None),
    }


@app.patch("/api/profile")
def update_profile(data: ProfileUpdateIn, username: str = Depends(get_current_username)):
    display_name = (data.display_name or "").strip()[:40]
    bio = (data.bio or "").strip()[:200]
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE users SET display_name=%s, bio=%s WHERE username=%s",
                (display_name or None, bio or None, username),
            )
        conn.commit()
    return {"ok": True, "display_name": display_name, "bio": bio}


@app.get("/api/stories")
def list_stories(username: str = Depends(get_current_username)):
    now = now_ts()
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM stories WHERE expires_at <= %s", (now,))
            cur.execute(
                """
                SELECT s.id, s.username, s.media_url, s.media_kind, s.caption, s.created_at, s.expires_at,
                       u.avatar_url, COALESCE(NULLIF(u.display_name, ''), u.username) AS display_name
                FROM stories s
                JOIN users u ON u.username = s.username
                WHERE s.expires_at > %s
                ORDER BY s.created_at DESC
                LIMIT 100
                """,
                (now,),
            )
            rows = cur.fetchall()
        conn.commit()
    return {"stories": rows}


@app.post("/api/stories")
async def create_story(
    file: UploadFile = File(...),
    caption: str = Form(""),
    username: str = Depends(get_current_username),
):
    content_type = (file.content_type or "").lower()
    kind = "image" if content_type.startswith("image/") else ("video" if content_type.startswith("video/") else "")
    if not kind:
        raise HTTPException(status_code=400, detail="Story supports image/video only")

    data = await file.read()
    if len(data) > MAX_UPLOAD_BYTES:
        raise HTTPException(status_code=413, detail=f"File too large (max {MAX_UPLOAD_MB}MB)")

    resource_type = "image" if kind == "image" else "video"
    try:
        up = cloudinary.uploader.upload(
            data,
            folder="messenger/stories",
            resource_type=resource_type,
        )
        url = up.get("secure_url") or up.get("url")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Cloudinary upload failed: {e}")

    now = now_ts()
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO stories(username, media_url, media_kind, caption, created_at, expires_at)
                VALUES (%s,%s,%s,%s,%s,%s)
                RETURNING id
                """,
                (username, url, kind, (caption or "").strip()[:160], now, now + 24 * 60 * 60),
            )
            story_id = int(cur.fetchone()["id"])
        conn.commit()
    return {"ok": True, "id": story_id, "media_url": url}


@app.delete("/api/stories/{story_id}")
def delete_story(story_id: int, username: str = Depends(get_current_username)):
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM stories WHERE id=%s AND username=%s", (story_id, username))
            if cur.rowcount == 0:
                raise HTTPException(status_code=404, detail="Story not found")
        conn.commit()
    return {"ok": True}


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
            overwrite=False,
            public_id=f"avatar_{username}_{now_ts()}",
        )
        url = up.get("secure_url") or up.get("url")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Cloudinary upload failed: {e}")

    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT avatar_url FROM users WHERE username=%s", (username,))
            prev = cur.fetchone()
            prev_avatar = (prev["avatar_url"] if prev else None)
            if prev_avatar:
                cur.execute(
                    "INSERT INTO user_avatar_history(username, avatar_url, created_at) VALUES(%s,%s,%s)",
                    (username, prev_avatar, now_ts()),
                )
            cur.execute("UPDATE users SET avatar_url=%s WHERE username=%s", (url, username))
        conn.commit()

    return {"ok": True, "avatar_url": url}


@app.get("/api/avatar/history")
def list_avatar_history(username: str = Depends(get_current_username)):
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, avatar_url, created_at
                FROM user_avatar_history
                WHERE username=%s
                ORDER BY created_at DESC
                LIMIT 30
                """,
                (username,),
            )
            rows = cur.fetchall()
    return {"items": rows}


@app.delete("/api/avatar/history/{item_id}")
def delete_avatar_history_item(item_id: int, username: str = Depends(get_current_username)):
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM user_avatar_history WHERE id=%s AND username=%s",
                (item_id, username),
            )
            if cur.rowcount == 0:
                raise HTTPException(status_code=404, detail="Avatar history item not found")
        conn.commit()
    return {"ok": True}


@app.get("/api/users/{target_username}/profile")
def user_profile(target_username: str, username: str = Depends(get_current_username)):
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT username, avatar_url,
                       COALESCE(NULLIF(display_name, ''), username) AS display_name,
                       bio
                FROM users
                WHERE username=%s
                """,
                (target_username,),
            )
            user_row = cur.fetchone()
            if not user_row:
                raise HTTPException(status_code=404, detail="User not found")

            cur.execute(
                """
                SELECT id, media_url, media_kind, caption, created_at, expires_at
                FROM stories
                WHERE username=%s AND expires_at > %s
                ORDER BY created_at DESC
                LIMIT 40
                """,
                (target_username, now_ts()),
            )
            story_rows = cur.fetchall()

            cur.execute(
                """
                SELECT id, avatar_url, created_at
                FROM user_avatar_history
                WHERE username=%s
                ORDER BY created_at DESC
                LIMIT 40
                """,
                (target_username,),
            )
            avatar_rows = cur.fetchall()

    return {
        "user": user_row,
        "stories": story_rows,
        "avatar_history": avatar_rows,
        "can_manage": target_username == username,
    }


@app.get("/api/contacts")
def list_contacts(username: str = Depends(get_current_username)):
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT c.contact_username AS username,
                       COALESCE(NULLIF(u.display_name, ''), u.username) AS display_name,
                       u.avatar_url
                FROM contacts c
                JOIN users u ON u.username = c.contact_username
                WHERE c.owner_username=%s
                ORDER BY c.created_at DESC
                """,
                (username,),
            )
            rows = cur.fetchall()
    for row in rows:
        row["online"] = row["username"] in USER_SOCKETS
    return {"contacts": rows}


@app.post("/api/contacts")
def add_contact(data: ContactCreateIn, username: str = Depends(get_current_username)):
    contact = (data.username or "").strip()
    if not USERNAME_RE.match(contact):
        raise HTTPException(status_code=400, detail="Invalid username")
    if contact == username:
        raise HTTPException(status_code=400, detail="Cannot add yourself")

    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM users WHERE username=%s", (contact,))
            if not cur.fetchone():
                raise HTTPException(status_code=404, detail="User not found")
            cur.execute(
                """
                INSERT INTO contacts(owner_username, contact_username, created_at)
                VALUES (%s,%s,%s)
                ON CONFLICT (owner_username, contact_username) DO NOTHING
                """,
                (username, contact, now_ts()),
            )
        conn.commit()
    return {"ok": True}


@app.delete("/api/contacts/{contact_username}")
def remove_contact(contact_username: str, username: str = Depends(get_current_username)):
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM contacts WHERE owner_username=%s AND contact_username=%s",
                (username, contact_username),
            )
        conn.commit()
    return {"ok": True}


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
                ),
                settings AS (
                    SELECT chat_id, muted_until
                    FROM chat_member_settings
                    WHERE username=%s
                )
                SELECT
                    mc.id, mc.type, mc.title, mc.created_by, mc.created_at,
                    lm.sender AS last_sender,
                    lm.text AS last_text,
                    lm.created_at AS last_created_at,
                    (SELECT s.muted_until FROM settings s WHERE s.chat_id = mc.id) AS muted_until,
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
                LEFT JOIN LATERAL (
                    SELECT m.sender, m.text, m.created_at
                    FROM messages m
                    LEFT JOIN message_hidden h
                      ON h.message_id = m.id AND h.username = %s
                    WHERE m.chat_id = mc.id
                      AND m.deleted_for_all = FALSE
                      AND h.message_id IS NULL
                    ORDER BY m.id DESC
                    LIMIT 1
                ) lm ON TRUE
                ORDER BY mc.created_at DESC
                """,
                (username, username, username, username, username, username),
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
                INSERT INTO chat_members(chat_id, username, role, joined_at)
                VALUES (%s,%s,%s,%s)
                ON CONFLICT (chat_id, username) DO UPDATE SET role='owner'
                """,
                (chat_id, username, "owner", now),
            )
            # Defensive cleanup: a new group must contain only creator.
            cur.execute("DELETE FROM chat_members WHERE chat_id=%s AND username<>%s", (chat_id, username))
            cur.execute("DELETE FROM chat_reads WHERE chat_id=%s AND username<>%s", (chat_id, username))
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
                    INSERT INTO chat_members(chat_id, username, role, joined_at)
                    VALUES (%s,%s,%s,%s)
                    ON CONFLICT (chat_id, username) DO NOTHING
                    """,
                    (chat_id, u, "admin", now),
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
        if not can_moderate(conn, chat_id, username):
            raise HTTPException(status_code=403, detail="Only owner/admin can invite")

        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM users WHERE username=%s", (other,))
            if cur.fetchone() is None:
                raise HTTPException(status_code=404, detail="User not found")

            now = now_ts()
            cur.execute(
                """
                INSERT INTO chat_members(chat_id, username, role, joined_at)
                VALUES (%s,%s,%s,%s)
                ON CONFLICT (chat_id, username) DO NOTHING
                """,
                (chat_id, other, "member", now),
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


@app.patch("/api/chats/{chat_id}/members/role")
async def update_member_role(
    chat_id: str,
    data: RoleUpdateIn,
    username: str = Depends(get_current_username),
):
    target = data.username.strip()
    role = data.role.strip().lower()
    if role not in ("admin", "member"):
        raise HTTPException(status_code=400, detail="role must be admin|member")

    with db() as conn:
        chat = get_chat(conn, chat_id)
        if not chat or chat["type"] != "group":
            raise HTTPException(status_code=404, detail="Group chat not found")
        if get_member_role(conn, chat_id, username) != "owner":
            raise HTTPException(status_code=403, detail="Only owner can change roles")
        if target == chat["created_by"]:
            raise HTTPException(status_code=400, detail="Cannot change owner role")

        with conn.cursor() as cur:
            cur.execute(
                "UPDATE chat_members SET role=%s WHERE chat_id=%s AND username=%s",
                (role, chat_id, target),
            )
            if cur.rowcount == 0:
                raise HTTPException(status_code=404, detail="Member not found")
        conn.commit()

    await broadcast_chat(chat_id, {"type": "role_updated", "chat_id": chat_id, "username": target, "role": role})
    return {"ok": True}


@app.delete("/api/chats/{chat_id}/members/{target}")
async def remove_member(
    chat_id: str,
    target: str,
    username: str = Depends(get_current_username),
):
    target = target.strip()
    with db() as conn:
        chat = get_chat(conn, chat_id)
        if not chat or chat["type"] != "group":
            raise HTTPException(status_code=404, detail="Group chat not found")
        if not can_moderate(conn, chat_id, username):
            raise HTTPException(status_code=403, detail="Only owner/admin can remove members")
        if target == chat["created_by"]:
            raise HTTPException(status_code=400, detail="Owner cannot be removed")

        with conn.cursor() as cur:
            cur.execute("DELETE FROM chat_members WHERE chat_id=%s AND username=%s", (chat_id, target))
            cur.execute("DELETE FROM chat_reads WHERE chat_id=%s AND username=%s", (chat_id, target))
        conn.commit()

    await broadcast_chat(chat_id, {"type": "member_removed", "chat_id": chat_id, "username": target})
    await broadcast_users([target], {"type": "chat_deleted", "chat_id": chat_id})
    return {"ok": True}


@app.post("/api/chats/{chat_id}/mute")
def mute_chat(
    chat_id: str,
    data: MuteIn,
    username: str = Depends(get_current_username),
):
    minutes = max(0, min(int(data.muted_minutes), 60 * 24 * 30))
    with db() as conn:
        if not is_member(conn, chat_id, username):
            raise HTTPException(status_code=403, detail="Not a member")
        muted_until = (now_ts() + minutes * 60) if minutes else None
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO chat_member_settings(chat_id, username, muted_until)
                VALUES (%s,%s,%s)
                ON CONFLICT (chat_id, username)
                DO UPDATE SET muted_until=EXCLUDED.muted_until
                """,
                (chat_id, username, muted_until),
            )
        conn.commit()
    return {"ok": True, "muted_until": muted_until}


@app.get("/api/chats/{chat_id}/pins")
def list_pins(chat_id: str, username: str = Depends(get_current_username)):
    with db() as conn:
        if not is_member(conn, chat_id, username):
            raise HTTPException(status_code=403, detail="Not a member")
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT p.message_id, p.pinned_by, p.pinned_at, m.text, m.sender
                FROM chat_pins p
                JOIN messages m ON m.id = p.message_id
                WHERE p.chat_id=%s
                ORDER BY p.pinned_at DESC
                LIMIT 20
                """,
                (chat_id,),
            )
            rows = cur.fetchall()
    return {"pins": rows}


@app.post("/api/chats/{chat_id}/pins")
async def pin_message(
    chat_id: str,
    data: PinIn,
    username: str = Depends(get_current_username),
):
    message_id = int(data.message_id)
    with db() as conn:
        chat = get_chat(conn, chat_id)
        if not chat:
            raise HTTPException(status_code=404, detail="Chat not found")
        if not can_moderate(conn, chat_id, username):
            raise HTTPException(status_code=403, detail="Only owner/admin can pin")
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM messages WHERE id=%s AND chat_id=%s", (message_id, chat_id))
            if not cur.fetchone():
                raise HTTPException(status_code=404, detail="Message not found")
            cur.execute(
                "INSERT INTO chat_pins(chat_id, message_id, pinned_by, pinned_at) VALUES (%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                (chat_id, message_id, username, now_ts()),
            )
        conn.commit()
    await broadcast_chat(chat_id, {"type": "pin_added", "chat_id": chat_id, "message_id": message_id})
    return {"ok": True}


@app.delete("/api/chats/{chat_id}/pins/{message_id}")
async def unpin_message(chat_id: str, message_id: int, username: str = Depends(get_current_username)):
    with db() as conn:
        if not can_moderate(conn, chat_id, username):
            raise HTTPException(status_code=403, detail="Only owner/admin can unpin")
        with conn.cursor() as cur:
            cur.execute("DELETE FROM chat_pins WHERE chat_id=%s AND message_id=%s", (chat_id, message_id))
        conn.commit()
    await broadcast_chat(chat_id, {"type": "pin_removed", "chat_id": chat_id, "message_id": int(message_id)})
    return {"ok": True}


@app.delete("/api/chats/{chat_id}")
async def delete_chat(chat_id: str, username: str = Depends(get_current_username)):
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


@app.get("/api/chats/{chat_id}/overview")
def chat_overview(
    chat_id: str,
    q: str = Query(default="", max_length=80),
    username: str = Depends(get_current_username),
):
    query = (q or "").strip()
    with db() as conn:
        if not is_member(conn, chat_id, username):
            raise HTTPException(status_code=403, detail="Not a member")
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT m.id, m.sender, m.text, m.created_at
                FROM messages m
                WHERE m.chat_id=%s
                  AND m.deleted_for_all=FALSE
                  AND (%s='' OR LOWER(m.text) LIKE LOWER(%s))
                ORDER BY m.id DESC
                LIMIT 40
                """,
                (chat_id, query, f"%{query}%"),
            )
            found_messages = cur.fetchall()

            cur.execute(
                """
                SELECT id, media_kind, media_url, media_name, sender, created_at
                FROM messages
                WHERE chat_id=%s
                  AND deleted_for_all=FALSE
                  AND media_url IS NOT NULL
                ORDER BY id DESC
                LIMIT 80
                """,
                (chat_id,),
            )
            media = cur.fetchall()

            cur.execute(
                """
                SELECT id, sender, text, created_at
                FROM messages
                WHERE chat_id=%s
                  AND deleted_for_all=FALSE
                  AND (
                    text ~* '(https?://[^\\s]+)'
                    OR text LIKE 'www.%'
                  )
                ORDER BY id DESC
                LIMIT 80
                """,
                (chat_id,),
            )
            links = cur.fetchall()

            cur.execute(
                """
                SELECT m.username,
                       m.role,
                       COALESCE(NULLIF(u.display_name, ''), u.username) AS display_name,
                       u.avatar_url
                FROM chat_members m
                JOIN users u ON u.username = m.username
                WHERE m.chat_id=%s
                ORDER BY m.joined_at ASC
                """,
                (chat_id,),
            )
            members = cur.fetchall()

    for m in members:
        m["online"] = m["username"] in USER_SOCKETS

    return {
        "messages": found_messages,
        "media": media,
        "links": links,
        "members": members,
    }


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

            elif t in {"call_offer", "call_answer", "call_reject", "call_end", "call_timeout"}:
                chat_id = (data.get("chat_id") or "").strip()
                call_id = str(data.get("call_id") or "").strip()
                mode = str(data.get("mode") or "voice").strip().lower()
                if not chat_id or not call_id:
                    continue
                with db() as conn:
                    if not is_member(conn, chat_id, username):
                        continue
                payload = {
                    "type": t,
                    "chat_id": chat_id,
                    "call_id": call_id,
                    "mode": "video" if mode == "video" else "voice",
                    "username": username,
                    "started_at": int(data.get("started_at") or now_ts()),
                    "duration": int(data.get("duration") or 0),
                    "reason": str(data.get("reason") or "").strip(),
                }
                await broadcast_chat(chat_id, payload)
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
                  m.media_kind, m.media_url, m.media_mime, m.media_name,
                  u.avatar_url AS sender_avatar_url,
                  m.reply_to_id,
                  r.sender AS reply_sender,
                  r.text AS reply_text
                FROM messages m
                LEFT JOIN users u ON u.username = m.sender
                LEFT JOIN messages r ON r.id = m.reply_to_id
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

        with conn.cursor() as cur:
            cur.execute(
                "SELECT message_id, emoji, COUNT(*) AS cnt FROM message_reactions WHERE message_id = ANY(%s) GROUP BY message_id, emoji",
                ([int(r["id"]) for r in rows] or [0],),
            )
            react_rows = cur.fetchall()

            cur.execute(
                "SELECT message_id, emoji FROM message_reactions WHERE message_id = ANY(%s) AND username=%s",
                ([int(r["id"]) for r in rows] or [0], username),
            )
            mine_rows = cur.fetchall()

    by_mid: Dict[int, Dict[str, int]] = {}
    for rr in react_rows:
        by_mid.setdefault(int(rr["message_id"]), {})[rr["emoji"]] = int(rr["cnt"])

    my_by_mid: Dict[int, List[str]] = {}
    for rr in mine_rows:
        my_by_mid.setdefault(int(rr["message_id"]), []).append(rr["emoji"])

    for r in rows:
        r["reactions"] = by_mid.get(int(r["id"]), {})
        r["my_reactions"] = my_by_mid.get(int(r["id"]), [])

    return {"messages": rows}


@app.get("/api/messages/{message_id}/status")
def get_message_status(
    message_id: int,
    username: str = Depends(get_current_username),
):
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT chat_id, sender FROM messages WHERE id=%s", (message_id,))
            msg = cur.fetchone()
            if not msg:
                raise HTTPException(status_code=404, detail="Message not found")
            chat_id = msg["chat_id"]
            sender = msg["sender"]

            if not is_member(conn, chat_id, username):
                raise HTTPException(status_code=403, detail="Not a member")

            cur.execute(
                """
                SELECT m.username,
                       d.delivered_at,
                       CASE WHEN r.last_read_id >= %s THEN r.updated_at ELSE NULL END AS read_at
                FROM chat_members m
                LEFT JOIN message_delivered d
                  ON d.message_id=%s AND d.username=m.username
                LEFT JOIN chat_reads r
                  ON r.chat_id=%s AND r.username=m.username
                WHERE m.chat_id=%s
                ORDER BY m.joined_at ASC
                """,
                (message_id, message_id, chat_id, chat_id),
            )
            rows = cur.fetchall()

    members = []
    delivered_count = 0
    read_count = 0
    delivered_latest = 0
    read_latest = 0

    for r in rows:
        u = r["username"]
        if u == sender:
            continue
        delivered_at = int(r["delivered_at"] or 0)
        read_at = int(r["read_at"] or 0)
        if delivered_at > 0:
            delivered_count += 1
            delivered_latest = max(delivered_latest, delivered_at)
        if read_at > 0:
            read_count += 1
            read_latest = max(read_latest, read_at)
        members.append({
            "username": u,
            "delivered_at": (delivered_at or None),
            "read_at": (read_at or None),
        })

    return {
        "ok": True,
        "message_id": int(message_id),
        "chat_id": chat_id,
        "sender": sender,
        "members_total": len(members),
        "delivered_count": delivered_count,
        "read_count": read_count,
        "delivered_latest": (delivered_latest or None),
        "read_latest": (read_latest or None),
        "members": members,
    }


@app.post("/api/messages")
async def create_text_message(
    data: MessageCreateIn,
    username: str = Depends(get_current_username),
):
    check_rate_limit(f"send:{username}", RATE_LIMIT_MAX_SEND)
    chat_id = (data.chat_id or "").strip()
    text = (data.text or "").strip()
    reply_to_id = int(data.reply_to_id or 0)

    if not chat_id:
        raise HTTPException(status_code=400, detail="chat_id required")
    if not text:
        raise HTTPException(status_code=400, detail="text required")
    if len(text) > 2000:
        raise HTTPException(status_code=400, detail="text too long (max 2000)")

    ts = now_ts()

    with db() as conn:
        sender_avatar_url = None
        if not is_member(conn, chat_id, username):
            raise HTTPException(status_code=403, detail="Not a member")

        with conn.cursor() as cur:
            cur.execute("SELECT avatar_url FROM users WHERE username=%s", (username,))
            user_row = cur.fetchone()
            sender_avatar_url = user_row["avatar_url"] if user_row else None

            reply_sender = None
            reply_text = None
            if reply_to_id > 0:
                cur.execute("SELECT id, sender, text FROM messages WHERE id=%s AND chat_id=%s", (reply_to_id, chat_id))
                rep = cur.fetchone()
                if not rep:
                    raise HTTPException(status_code=400, detail="reply_to message not found")
                reply_sender = rep["sender"]
                reply_text = (rep["text"] or "")[:160]

            cur.execute(
                """
                INSERT INTO messages(chat_id, sender, text, created_at, reply_to_id)
                VALUES (%s,%s,%s,%s,%s)
                RETURNING id
                """,
                (chat_id, username, text, ts, reply_to_id if reply_to_id > 0 else None),
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
        "sender_avatar_url": sender_avatar_url,
        "text": text,
        "created_at": ts,
        "is_edited": False,
        "deleted_for_all": False,
        "media_kind": None,
        "media_url": None,
        "media_mime": None,
        "media_name": None,
        "reply_to_id": (reply_to_id if reply_to_id > 0 else None),
        "reply_sender": reply_sender,
        "reply_text": reply_text,
        "reactions": {},
    }
    await broadcast_chat(chat_id, payload)
    return {"ok": True, "id": msg_id}


@app.post("/api/messages/{message_id}/forward")
async def forward_message(
    message_id: int,
    data: ForwardIn,
    username: str = Depends(get_current_username),
):
    check_rate_limit(f"send:{username}", RATE_LIMIT_MAX_SEND)
    target_chat_id = (data.target_chat_id or "").strip()
    if not target_chat_id:
        raise HTTPException(status_code=400, detail="target_chat_id required")

    ts = now_ts()

    with db() as conn:
        sender_avatar_url = None
        with conn.cursor() as cur:
            cur.execute("SELECT avatar_url FROM users WHERE username=%s", (username,))
            user_row = cur.fetchone()
            sender_avatar_url = user_row["avatar_url"] if user_row else None

            cur.execute(
                """
                SELECT chat_id, sender, text, media_kind, media_url, media_mime, media_name, deleted_for_all
                FROM messages
                WHERE id=%s
                """,
                (message_id,),
            )
            src = cur.fetchone()
            if not src:
                raise HTTPException(status_code=404, detail="Message not found")

            source_chat_id = src["chat_id"]
            if not is_member(conn, source_chat_id, username):
                raise HTTPException(status_code=403, detail="Not a member of source chat")
            if not is_member(conn, target_chat_id, username):
                raise HTTPException(status_code=403, detail="Not a member of target chat")
            if src["deleted_for_all"]:
                raise HTTPException(status_code=400, detail="Cannot forward deleted message")

            original_sender = (src["sender"] or "user").strip()
            original_text = (src["text"] or "").strip()
            prefix = f"↪ Forwarded from {original_sender}: "
            body_text = (prefix + original_text).strip()
            if len(body_text) > 2000:
                body_text = body_text[:2000]

            cur.execute(
                """
                INSERT INTO messages(chat_id, sender, text, created_at, media_kind, media_url, media_mime, media_name)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                RETURNING id
                """,
                (
                    target_chat_id,
                    username,
                    body_text,
                    ts,
                    src["media_kind"],
                    src["media_url"],
                    src["media_mime"],
                    src["media_name"],
                ),
            )
            new_id = int(cur.fetchone()["id"])

            cur.execute(
                """
                INSERT INTO message_delivered(message_id, username, delivered_at)
                VALUES (%s,%s,%s)
                ON CONFLICT DO NOTHING
                """,
                (new_id, username, ts),
            )
        conn.commit()

    payload = {
        "type": "message",
        "id": new_id,
        "chat_id": target_chat_id,
        "sender": username,
        "sender_avatar_url": sender_avatar_url,
        "text": body_text,
        "created_at": ts,
        "is_edited": False,
        "deleted_for_all": False,
        "media_kind": src["media_kind"],
        "media_url": src["media_url"],
        "media_mime": src["media_mime"],
        "media_name": src["media_name"],
        "reply_to_id": None,
        "reply_sender": None,
        "reply_text": None,
        "reactions": {},
    }
    await broadcast_chat(target_chat_id, payload)
    return {"ok": True, "id": new_id}


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


@app.post("/api/messages/{message_id}/reactions")
async def add_reaction(
    message_id: int,
    data: ReactionIn,
    username: str = Depends(get_current_username),
):
    emoji = (data.emoji or "").strip()[:16]
    if not emoji:
        raise HTTPException(status_code=400, detail="emoji required")

    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT chat_id FROM messages WHERE id=%s", (message_id,))
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Message not found")
            chat_id = row["chat_id"]
            if not is_member(conn, chat_id, username):
                raise HTTPException(status_code=403, detail="Not a member")
            cur.execute(
                """
                INSERT INTO message_reactions(message_id, username, emoji, created_at)
                VALUES (%s,%s,%s,%s)
                ON CONFLICT DO NOTHING
                """,
                (message_id, username, emoji, now_ts()),
            )
        conn.commit()

    await broadcast_chat(chat_id, {"type": "reaction_added", "chat_id": chat_id, "message_id": message_id, "emoji": emoji, "username": username})
    return {"ok": True}


@app.delete("/api/messages/{message_id}/reactions")
async def remove_reaction(
    message_id: int,
    emoji: str = Query(...),
    username: str = Depends(get_current_username),
):
    emoji = (emoji or "").strip()[:16]
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT chat_id FROM messages WHERE id=%s", (message_id,))
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Message not found")
            chat_id = row["chat_id"]
            if not is_member(conn, chat_id, username):
                raise HTTPException(status_code=403, detail="Not a member")
            cur.execute("DELETE FROM message_reactions WHERE message_id=%s AND username=%s AND emoji=%s", (message_id, username, emoji))
        conn.commit()

    await broadcast_chat(chat_id, {"type": "reaction_removed", "chat_id": chat_id, "message_id": message_id, "emoji": emoji, "username": username})
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
    check_rate_limit(f"send:{username}", RATE_LIMIT_MAX_SEND)
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
        sender_avatar_url = None
        with conn.cursor() as cur:
            cur.execute("SELECT avatar_url FROM users WHERE username=%s", (username,))
            user_row = cur.fetchone()
            sender_avatar_url = user_row["avatar_url"] if user_row else None

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
        "sender_avatar_url": sender_avatar_url,
        "text": caption,
        "created_at": ts,
        "is_edited": False,
        "deleted_for_all": False,
        "media_kind": kind,
        "media_url": url,
        "media_mime": content_type,
        "media_name": media_name,
        "reply_to_id": None,
        "reply_sender": None,
        "reply_text": None,
        "reactions": {},
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
