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
BACKEND_DIR = os.path.dirname(os.path.abspath(__file__))     # .../backend
PROJECT_ROOT = os.path.dirname(BACKEND_DIR)                  # .../
FRONTEND_DIR = os.path.join(PROJECT_ROOT, "frontend")        # .../frontend


# =========================
# Config
# =========================
JWT_SECRET = os.environ.get("JWT_SECRET", "dev-secret-change-me")
JWT_TTL_SECONDS = int(os.environ.get("JWT_TTL_SECONDS", str(60 * 60 * 24 * 30)))  # 30 days

DATABASE_URL = (os.environ.get("DATABASE_URL") or "").strip()
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL is required")

CLOUDINARY_CLOUD_NAME = os.environ.get("CLOUDINARY_CLOUD_NAME", "").strip()
CLOUDINARY_API_KEY = os.environ.get("CLOUDINARY_API_KEY", "").strip()
CLOUDINARY_API_SECRET = os.environ.get("CLOUDINARY_API_SECRET", "").strip()

if not (CLOUDINARY_CLOUD_NAME and CLOUDINARY_API_KEY and CLOUDINARY_API_SECRET):
    raise RuntimeError("Cloudinary env vars are required: CLOUDINARY_CLOUD_NAME, CLOUDINARY_API_KEY, CLOUDINARY_API_SECRET")

cloudinary.config(
    cloud_name=CLOUDINARY_CLOUD_NAME,
    api_key=CLOUDINARY_API_KEY,
    api_secret=CLOUDINARY_API_SECRET,
    secure=True,
)

USERNAME_RE = re.compile(r"^[a-zA-Z0-9_]{3,32}$")


# =========================
# DB helpers
# =========================
def db():
    return psycopg.connect(DATABASE_URL, row_factory=dict_row)


def init_db():
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    id SERIAL PRIMARY KEY,
                    username TEXT UNIQUE NOT NULL,
                    pass_hash TEXT NOT NULL,
                    created_at BIGINT NOT NULL
                );
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS chats (
                    id TEXT PRIMARY KEY,
                    title TEXT NOT NULL,
                    type TEXT NOT NULL,
                    created_by TEXT NOT NULL,
                    created_at BIGINT NOT NULL
                );
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS chat_members (
                    chat_id TEXT NOT NULL,
                    username TEXT NOT NULL,
                    PRIMARY KEY(chat_id, username)
                );
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS messages (
                    id BIGSERIAL PRIMARY KEY,
                    chat_id TEXT NOT NULL,
                    username TEXT NOT NULL,
                    text TEXT NOT NULL DEFAULT '',
                    media_url TEXT NOT NULL DEFAULT '',
                    media_kind TEXT NOT NULL DEFAULT '',
                    created_at BIGINT NOT NULL
                );
            """)
            conn.commit()


# =========================
# JWT (simple)
# =========================
def b64url(data: bytes) -> str:
    import base64
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("utf-8")


def b64url_decode(s: str) -> bytes:
    import base64
    pad = "=" * (-len(s) % 4)
    return base64.urlsafe_b64decode((s + pad).encode("utf-8"))


def jwt_sign(payload: dict) -> str:
    header = {"alg": "HS256", "typ": "JWT"}
    h = b64url(json.dumps(header, separators=(",", ":")).encode("utf-8"))
    p = b64url(json.dumps(payload, separators=(",", ":")).encode("utf-8"))
    msg = f"{h}.{p}".encode("utf-8")
    sig = hmac.new(JWT_SECRET.encode("utf-8"), msg, hashlib.sha256).digest()
    return f"{h}.{p}.{b64url(sig)}"


def jwt_verify(token: str) -> dict:
    try:
        h, p, s = token.split(".")
        msg = f"{h}.{p}".encode("utf-8")
        sig = b64url_decode(s)
        exp_sig = hmac.new(JWT_SECRET.encode("utf-8"), msg, hashlib.sha256).digest()
        if not hmac.compare_digest(sig, exp_sig):
            raise ValueError("bad signature")
        payload = json.loads(b64url_decode(p))
        if payload.get("exp", 0) < int(time.time()):
            raise ValueError("expired")
        return payload
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid token")


def hash_password(pw: str) -> str:
    salt = secrets.token_hex(16)
    h = hashlib.sha256((salt + pw).encode("utf-8")).hexdigest()
    return f"{salt}${h}"


def verify_password(pw: str, stored: str) -> bool:
    try:
        salt, h = stored.split("$", 1)
        return hashlib.sha256((salt + pw).encode("utf-8")).hexdigest() == h
    except Exception:
        return False


def get_token_user(auth: Optional[str] = Header(default=None)) -> str:
    if not auth or not auth.lower().startswith("bearer "):
        raise HTTPException(status_code=401, detail="No token")
    tok = auth.split(" ", 1)[1].strip()
    payload = jwt_verify(tok)
    return payload["u"]


# =========================
# FastAPI app
# =========================
app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Serve frontend
if os.path.isdir(FRONTEND_DIR):
    app.mount("/", StaticFiles(directory=FRONTEND_DIR, html=True), name="frontend")

# init DB
init_db()


# =========================
# WebSocket manager
# =========================
class WSManager:
    def __init__(self):
        self.clients: Set[WebSocket] = set()

    async def connect(self, ws: WebSocket):
        await ws.accept()
        self.clients.add(ws)

    def disconnect(self, ws: WebSocket):
        if ws in self.clients:
            self.clients.remove(ws)

    async def broadcast(self, data: dict):
        dead = []
        for c in list(self.clients):
            try:
                await c.send_text(json.dumps(data))
            except Exception:
                dead.append(c)
        for c in dead:
            self.disconnect(c)


ws_manager = WSManager()


# =========================
# Models
# =========================
class AuthIn(BaseModel):
    username: str
    password: str


class ChatCreateIn(BaseModel):
    title: str


class DmCreateIn(BaseModel):
    username: str


class MsgCreateIn(BaseModel):
    chat_id: str
    text: str


# =========================
# Auth endpoints
# =========================
@app.post("/api/auth/register")
def register(data: AuthIn):
    u = data.username.strip()
    p = data.password.strip()

    if not USERNAME_RE.match(u):
        raise HTTPException(status_code=400, detail="Bad username (3-32 chars: letters/digits/_)")
    if len(p) < 4:
        raise HTTPException(status_code=400, detail="Password too short")

    ph = hash_password(p)
    now = int(time.time())

    with db() as conn:
        with conn.cursor() as cur:
            try:
                cur.execute("INSERT INTO users(username, pass_hash, created_at) VALUES (%s,%s,%s)", (u, ph, now))
                conn.commit()
            except Exception:
                raise HTTPException(status_code=400, detail="Username already exists")

    payload = {"u": u, "exp": int(time.time()) + JWT_TTL_SECONDS}
    token = jwt_sign(payload)
    return {"token": token, "username": u}


@app.post("/api/auth/login")
def login(data: AuthIn):
    u = data.username.strip()
    p = data.password.strip()

    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM users WHERE username=%s", (u,))
            row = cur.fetchone()
            if not row or not verify_password(p, row["pass_hash"]):
                raise HTTPException(status_code=400, detail="Invalid credentials")

    payload = {"u": u, "exp": int(time.time()) + JWT_TTL_SECONDS}
    token = jwt_sign(payload)
    return {"token": token, "username": u}


# =========================
# Chats
# =========================
def make_chat_id() -> str:
    return secrets.token_hex(12)


@app.get("/api/chats")
def list_chats(user: str = Depends(get_token_user)):
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT c.*
                FROM chats c
                JOIN chat_members m ON m.chat_id=c.id
                WHERE m.username=%s
                ORDER BY c.created_at DESC
            """, (user,))
            rows = cur.fetchall() or []
    return {"chats": rows}


@app.post("/api/chats")
async def create_chat(data: ChatCreateIn, user: str = Depends(get_token_user)):
    title = data.title.strip()
    if not title:
        raise HTTPException(status_code=400, detail="Title required")

    chat_id = make_chat_id()
    now = int(time.time())

    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("INSERT INTO chats(id,title,type,created_by,created_at) VALUES (%s,%s,%s,%s,%s)",
                        (chat_id, title, "group", user, now))
            cur.execute("INSERT INTO chat_members(chat_id,username) VALUES (%s,%s)", (chat_id, user))
            conn.commit()

    await ws_manager.broadcast({"type": "chat_created", "chat": {"id": chat_id, "title": title, "type": "group"}})
    return {"chat": {"id": chat_id, "title": title, "type": "group"}}


@app.post("/api/chats/dm")
async def create_dm(data: DmCreateIn, user: str = Depends(get_token_user)):
    other = data.username.strip()
    if not USERNAME_RE.match(other):
        raise HTTPException(status_code=400, detail="Bad username")
    if other == user:
        raise HTTPException(status_code=400, detail="Can't DM yourself")

    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT username FROM users WHERE username=%s", (other,))
            if not cur.fetchone():
                raise HTTPException(status_code=404, detail="User not found")

            # deterministic dm title key
            a, b = sorted([user, other])
            title = f"dm:{a}|{b}"

            # check if exists
            cur.execute("SELECT id FROM chats WHERE type='dm' AND title=%s", (title,))
            row = cur.fetchone()
            if row:
                chat_id = row["id"]
            else:
                chat_id = make_chat_id()
                now = int(time.time())
                cur.execute("INSERT INTO chats(id,title,type,created_by,created_at) VALUES (%s,%s,%s,%s,%s)",
                            (chat_id, title, "dm", user, now))
                cur.execute("INSERT INTO chat_members(chat_id,username) VALUES (%s,%s)", (chat_id, user))
                cur.execute("INSERT INTO chat_members(chat_id,username) VALUES (%s,%s)", (chat_id, other))
                conn.commit()

    await ws_manager.broadcast({"type": "chat_created", "chat": {"id": chat_id, "title": title, "type": "dm"}})
    return {"chat": {"id": chat_id, "title": title, "type": "dm"}}


# =========================
# Messages
# =========================
@app.get("/api/messages")
def list_messages(chat_id: str, user: str = Depends(get_token_user)):
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM chat_members WHERE chat_id=%s AND username=%s", (chat_id, user))
            if not cur.fetchone():
                raise HTTPException(status_code=403, detail="Not a member")

            cur.execute("""
                SELECT id, chat_id, username, text, media_url, media_kind, created_at
                FROM messages
                WHERE chat_id=%s
                ORDER BY id ASC
                LIMIT 500
            """, (chat_id,))
            rows = cur.fetchall() or []
    return {"messages": rows}


@app.post("/api/messages")
async def create_message(data: MsgCreateIn, user: str = Depends(get_token_user)):
    chat_id = data.chat_id
    text = (data.text or "").strip()
    if not text:
        raise HTTPException(status_code=400, detail="Text required")

    now = int(time.time())

    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM chat_members WHERE chat_id=%s AND username=%s", (chat_id, user))
            if not cur.fetchone():
                raise HTTPException(status_code=403, detail="Not a member")

            cur.execute("""
                INSERT INTO messages(chat_id, username, text, media_url, media_kind, created_at)
                VALUES (%s,%s,%s,'','',%s)
                RETURNING id
            """, (chat_id, user, text, now))
            msg_id = cur.fetchone()["id"]
            conn.commit()

    message = {
        "id": msg_id,
        "chat_id": chat_id,
        "username": user,
        "text": text,
        "media_url": "",
        "media_kind": "",
        "created_at": now
    }
    await ws_manager.broadcast({"type": "message", "chat_id": chat_id, "message": message})
    return {"ok": True, "message": message}


@app.post("/api/messages/upload")
async def upload_message(
    chat_id: str = Form(...),
    caption: str = Form(""),
    file: UploadFile = File(...),
    user: str = Depends(get_token_user),
):
    # membership check
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM chat_members WHERE chat_id=%s AND username=%s", (chat_id, user))
            if not cur.fetchone():
                raise HTTPException(status_code=403, detail="Not a member")

    # detect kind
    ctype = (file.content_type or "").lower()
    kind = ""
    if ctype.startswith("image/"):
        kind = "image"
    elif ctype.startswith("video/"):
        kind = "video"
    elif ctype.startswith("audio/") or ctype in ("application/ogg", "application/octet-stream"):
        kind = "audio"
    else:
        # allow some webm audio blobs
        if file.filename.lower().endswith(".webm") or file.filename.lower().endswith(".ogg"):
            kind = "audio"
        else:
            raise HTTPException(status_code=400, detail=f"Unsupported file type: {ctype}")

    # Cloudinary upload
    raw = await file.read()
    if not raw:
        raise HTTPException(status_code=400, detail="Empty file")

    # resource_type:
    # - images => image
    # - videos & audio often use 'video' on cloudinary
    resource_type = "image" if kind == "image" else "video"

    up = cloudinary.uploader.upload(
        raw,
        resource_type=resource_type,
        folder="messenger",
        public_id=f"{int(time.time())}_{secrets.token_hex(6)}",
        overwrite=False,
    )
    url = up.get("secure_url") or up.get("url") or ""
    if not url:
        raise HTTPException(status_code=500, detail="Upload failed")

    now = int(time.time())
    text = (caption or "").strip()

    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO messages(chat_id, username, text, media_url, media_kind, created_at)
                VALUES (%s,%s,%s,%s,%s,%s)
                RETURNING id
            """, (chat_id, user, text, url, kind, now))
            msg_id = cur.fetchone()["id"]
            conn.commit()

    message = {
        "id": msg_id,
        "chat_id": chat_id,
        "username": user,
        "text": text,
        "media_url": url,
        "media_kind": kind,
        "created_at": now
    }
    await ws_manager.broadcast({"type": "message", "chat_id": chat_id, "message": message})
    return {"ok": True, "message": message}


# =========================
# WebSocket endpoint
# =========================
@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):
    token = ws.query_params.get("token", "")
    if not token:
        await ws.close(code=1008)
        return
    try:
        payload = jwt_verify(token)
        _user = payload["u"]
    except Exception:
        await ws.close(code=1008)
        return

    await ws_manager.connect(ws)
    try:
        while True:
            # keep alive / ignore client messages
            await ws.receive_text()
    except WebSocketDisconnect:
        ws_manager.disconnect(ws)
    except Exception:
        ws_manager.disconnect(ws)
