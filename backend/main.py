from __future__ import annotations

import os
import time
import json
import re
import hmac
import hashlib
import secrets
from typing import Dict, Set, Optional

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
)
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel


# =========================
# Paths (MONOREPO)
# backend/main.py
# frontend/index.html
# =========================
BACKEND_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(BACKEND_DIR)
FRONTEND_DIR = os.path.join(PROJECT_ROOT, "frontend")


# =========================
# Config
# =========================
JWT_SECRET = os.environ.get("JWT_SECRET", "dev-secret-change-me")
JWT_TTL_SECONDS = int(os.environ.get("JWT_TTL_SECONDS", str(60 * 60 * 24 * 30)))  # 30 days

DATABASE_URL = (os.environ.get("DATABASE_URL") or "").strip()
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL env is required")

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
    return psycopg.connect(DATABASE_URL, row_factory=dict_row)


def init_db() -> None:
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS users (
                    id BIGSERIAL PRIMARY KEY,
                    username TEXT UNIQUE NOT NULL,
                    pass_hash TEXT NOT NULL,
                    created_at BIGINT NOT NULL
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
                    created_at BIGINT NOT NULL
                );
                """
            )
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
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS messages (
                    id BIGSERIAL PRIMARY KEY,
                    chat_id TEXT NOT NULL,
                    sender TEXT NOT NULL,
                    text TEXT NOT NULL,
                    created_at BIGINT NOT NULL,
                    media_kind TEXT,
                    media_url TEXT,
                    media_mime TEXT,
                    media_name TEXT
                );
                """
            )
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
                    ("general", "group", "General", "system", int(time.time())),
                )

            cur.execute(
                """
                INSERT INTO chat_members(chat_id, username, joined_at)
                VALUES (%s,%s,%s)
                ON CONFLICT (chat_id, username) DO NOTHING
                """,
                ("general", username, int(time.time())),
            )
        conn.commit()


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


def make_id(prefix: str = "") -> str:
    return prefix + secrets.token_urlsafe(12)


def media_kind_from_mime(mime: str) -> str:
    mime = (mime or "").lower().strip()
    if mime in ALLOWED_IMAGE_MIME or mime.startswith("image/"):
        return "image"
    if mime in ALLOWED_VIDEO_MIME or mime.startswith("video/"):
        return "video"
    if mime in ALLOWED_AUDIO_MIME or mime.startswith("audio/"):
        return "audio"
    return ""


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

class MsgIn(BaseModel):
    chat_id: str
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

    now = int(time.time())
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

    now = int(time.time())
    token = jwt_sign({"sub": username, "iat": now, "exp": now + JWT_TTL_SECONDS})
    return {"token": token, "username": username}


@app.get("/api/me")
def me(username: str = Depends(get_current_username)):
    return {"username": username}


# =========================
# Chats API
# =========================
@app.get("/api/chats")
def list_chats(username: str = Depends(get_current_username)):
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT c.id, c.type, c.title, c.created_at
                FROM chats c
                JOIN chat_members m ON m.chat_id = c.id
                WHERE m.username=%s
                ORDER BY c.created_at DESC
                """,
                (username,),
            )
            rows = cur.fetchall()
    return {"chats": rows}


@app.post("/api/chats")
def create_group_chat(data: ChatCreateIn, username: str = Depends(get_current_username)):
    title = data.title.strip()
    if not title or len(title) > 40:
        raise HTTPException(status_code=400, detail="Название чата: 1-40 символов.")

    chat_id = make_id("c_")
    now = int(time.time())

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
                ON CONFLICT (chat_id, username) DO NOTHING
                """,
                (chat_id, username, now),
            )
        conn.commit()

    return {"chat": {"id": chat_id, "type": "group", "title": title, "created_at": now}}


@app.post("/api/chats/dm")
def create_dm(data: DMCreateIn, username: str = Depends(get_current_username)):
    other = data.username.strip()
    if not USERNAME_RE.match(other):
        raise HTTPException(status_code=400, detail="Некорректный username.")
    if other == username:
        raise HTTPException(status_code=400, detail="Нельзя создать DM с самим собой.")

    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM users WHERE username=%s LIMIT 1", (other,))
            if cur.fetchone() is None:
                raise HTTPException(status_code=404, detail="Пользователь не найден.")

            a, b = sorted([username, other])
            dm_key = f"dm:{a}|{b}"

            cur.execute("SELECT id, created_at FROM chats WHERE type='dm' AND title=%s", (dm_key,))
            row = cur.fetchone()
            if row:
                chat_id = row["id"]
                created_at = row["created_at"]
            else:
                chat_id = make_id("d_")
                created_at = int(time.time())
                cur.execute(
                    "INSERT INTO chats(id, type, title, created_by, created_at) VALUES(%s,%s,%s,%s,%s)",
                    (chat_id, "dm", dm_key, username, created_at),
                )

            now = int(time.time())
            cur.execute(
                """
                INSERT INTO chat_members(chat_id, username, joined_at)
                VALUES (%s,%s,%s)
                ON CONFLICT (chat_id, username) DO NOTHING
                """,
                (chat_id, username, now),
            )
            cur.execute(
                """
                INSERT INTO chat_members(chat_id, username, joined_at)
                VALUES (%s,%s,%s)
                ON CONFLICT (chat_id, username) DO NOTHING
                """,
                (chat_id, other, now),
            )

        conn.commit()

    return {"chat": {"id": chat_id, "type": "dm", "title": f"DM: {other}", "created_at": created_at}}


# =========================
# Messages API
# =========================
@app.get("/api/messages")
def list_messages(chat_id: str, username: str = Depends(get_current_username)):
    with db() as conn:
        if not is_member(conn, chat_id, username):
            raise HTTPException(status_code=403, detail="Нет доступа к этому чату.")

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, chat_id, sender, text, created_at, media_kind, media_url, media_mime, media_name
                FROM messages
                WHERE chat_id=%s
                ORDER BY id DESC
                LIMIT 200
                """,
                (chat_id,),
            )
            rows = cur.fetchall()

    return {"chat_id": chat_id, "messages": list(reversed(rows))}


@app.post("/api/messages")
async def create_text_message(data: MsgIn, username: str = Depends(get_current_username)):
    chat_id = (data.chat_id or "").strip()
    text = (data.text or "").strip()

    if not text or len(text) > 2000:
        raise HTTPException(status_code=400, detail="Текст: 1-2000 символов.")

    with db() as conn:
        if not is_member(conn, chat_id, username):
            raise HTTPException(status_code=403, detail="Нет доступа к этому чату.")

        ts = int(time.time())
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO messages(chat_id, sender, text, created_at, media_kind, media_url, media_mime, media_name)
                VALUES(%s,%s,%s,%s,NULL,NULL,NULL,NULL)
                RETURNING id
                """,
                (chat_id, username, text, ts),
            )
            msg_id = cur.fetchone()["id"]
        conn.commit()

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
    return {"message": event}


@app.post("/api/upload")
async def upload(
    chat_id: str = Form(...),
    text: str = Form(""),
    file: UploadFile = File(...),
    username: str = Depends(get_current_username),
):
    text = (text or "").strip()
    if len(text) > 2000:
        raise HTTPException(status_code=400, detail="Текст: максимум 2000 символов.")

    with db() as conn:
        if not is_member(conn, chat_id, username):
            raise HTTPException(status_code=403, detail="Нет доступа к этому чату.")

    mime = (file.content_type or "").lower().strip()
    kind = media_kind_from_mime(mime)
    if not kind:
        raise HTTPException(status_code=400, detail=f"Неподдерживаемый тип файла: {mime or 'unknown'}")

    data = await file.read()
    if not data:
        raise HTTPException(status_code=400, detail="Пустой файл.")
    if len(data) > MAX_UPLOAD_BYTES:
        raise HTTPException(status_code=400, detail=f"Слишком большой файл. Лимит: {MAX_UPLOAD_MB} MB")

    folder = "mini_messenger"
    public_id = f"{folder}/{chat_id}/{int(time.time())}_{secrets.token_urlsafe(10)}"

    try:
        uploaded = cloudinary.uploader.upload(
            data,
            resource_type="auto",
            public_id=public_id,
            overwrite=False,
            unique_filename=True,
        )
        media_url = uploaded.get("secure_url") or uploaded.get("url")
        if not media_url:
            raise RuntimeError("Cloudinary returned no url")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Cloudinary upload failed: {e}")

    ts = int(time.time())

    with db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO messages(chat_id, sender, text, created_at, media_kind, media_url, media_mime, media_name)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                RETURNING id
                """,
                (
                    chat_id,
                    username,
                    text,
                    ts,
                    kind,
                    media_url,
                    mime,
                    (file.filename or "")[:200],
                ),
            )
            msg_id = cur.fetchone()["id"]
        conn.commit()

    event = {
        "type": "message",
        "id": msg_id,
        "chat_id": chat_id,
        "sender": username,
        "text": text,
        "created_at": ts,
        "media_kind": kind,
        "media_url": media_url,
        "media_mime": mime,
        "media_name": (file.filename or "")[:200],
    }

    await broadcast(chat_id, event)
    return {"message": event}


# =========================
# WebSocket connections
# =========================
connections: Dict[str, Set[WebSocket]] = {}

def add_conn(chat_id: str, ws: WebSocket) -> None:
    connections.setdefault(chat_id, set()).add(ws)

def remove_conn(chat_id: str, ws: WebSocket) -> None:
    if chat_id in connections:
        connections[chat_id].discard(ws)
        if not connections[chat_id]:
            connections.pop(chat_id, None)

async def broadcast(chat_id: str, event: dict) -> None:
    conns = list(connections.get(chat_id, set()))
    dead: list[WebSocket] = []
    for c in conns:
        try:
            await c.send_text(json.dumps(event))
        except Exception:
            dead.append(c)
    for d in dead:
        remove_conn(chat_id, d)


@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):
    token = (ws.query_params.get("token") or "").strip()
    chat_id = (ws.query_params.get("chat_id") or "").strip()

    if not token or not chat_id:
        await ws.close(code=4401)
        return

    try:
        username = jwt_verify(token)["sub"]
    except HTTPException:
        await ws.close(code=4401)
        return

    with db() as conn:
        if not is_member(conn, chat_id, username):
            await ws.close(code=4403)
            return

    await ws.accept()
    add_conn(chat_id, ws)

    try:
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        remove_conn(chat_id, ws)


# =========================
# Frontend serve — MUST BE LAST
# =========================
if os.path.isdir(FRONTEND_DIR):
    app.mount("/", StaticFiles(directory=FRONTEND_DIR, html=True), name="frontend")
else:
    @app.get("/")
    def _no_frontend():
        return {
            "error": "frontend folder not found",
            "expected_path": FRONTEND_DIR,
            "hint": "Repo should contain: frontend/index.html рядом с backend/",
        }
