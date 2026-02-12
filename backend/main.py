from __future__ import annotations

import os
import time
import json
import sqlite3
import hashlib
import hmac
import secrets
import re
from typing import Dict, Set, Optional, List

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
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


# =========================
# DB
# =========================
def db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


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

    # chats: id=uuid string, type=group|dm
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

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id TEXT NOT NULL,
            sender TEXT NOT NULL,
            text TEXT NOT NULL,
            created_at INTEGER NOT NULL
        );
        """
    )

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
# FastAPI app
# =========================
init_db()
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


# =========================
# Schemas
# =========================
class AuthIn(BaseModel):
    username: str
    password: str


class ChatCreateIn(BaseModel):
    title: str


class DMCreateIn(BaseModel):
    username: str  # other user


USERNAME_RE = re.compile(r"^[a-zA-Z0-9_]{3,20}$")


def require_user(token: str) -> str:
    payload = jwt_verify(token)
    return payload["sub"]


def make_id(prefix: str = "") -> str:
    return prefix + secrets.token_urlsafe(12)


def is_member(conn: sqlite3.Connection, chat_id: str, username: str) -> bool:
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM chat_members WHERE chat_id=? AND username=? LIMIT 1", (chat_id, username))
    return cur.fetchone() is not None


def ensure_default_chat_for(conn: sqlite3.Connection, username: str) -> None:
    # Create global chat once, add all users on first login/register
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


# =========================
# Chats
# =========================
@app.get("/api/chats")
def list_chats(token: str):
    me = require_user(token)
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
        (me,),
    )
    rows = cur.fetchall()
    conn.close()
    return {"chats": [dict(r) for r in rows]}


@app.post("/api/chats")
def create_chat(token: str, data: ChatCreateIn):
    me = require_user(token)
    title = data.title.strip()
    if not title or len(title) > 40:
        raise HTTPException(status_code=400, detail="Название чата: 1-40 символов.")

    chat_id = make_id("c_")
    now = int(time.time())

    conn = db()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO chats(id, type, title, created_by, created_at) VALUES(?,?,?,?,?)",
        (chat_id, "group", title, me, now),
    )
    cur.execute(
        "INSERT INTO chat_members(chat_id, username, joined_at) VALUES(?,?,?)",
        (chat_id, me, now),
    )
    conn.commit()
    conn.close()
    return {"chat": {"id": chat_id, "type": "group", "title": title, "created_at": now}}


@app.post("/api/chats/dm")
def create_dm(token: str, data: DMCreateIn):
    me = require_user(token)
    other = data.username.strip()
    if not USERNAME_RE.match(other):
        raise HTTPException(status_code=400, detail="Некорректный username.")
    if other == me:
        raise HTTPException(status_code=400, detail="Нельзя создать DM с самим собой.")

    conn = db()
    cur = conn.cursor()

    # check other exists
    cur.execute("SELECT 1 FROM users WHERE username=? LIMIT 1", (other,))
    if cur.fetchone() is None:
        conn.close()
        raise HTTPException(status_code=404, detail="Пользователь не найден.")

    # Find existing dm by title key "dm:me|other" (sorted)
    a, b = sorted([me, other])
    dm_key = f"dm:{a}|{b}"

    cur.execute("SELECT id, title, created_at FROM chats WHERE type='dm' AND title=?", (dm_key,))
    row = cur.fetchone()
    if row:
        chat_id = row["id"]
        created_at = row["created_at"]
    else:
        chat_id = make_id("d_")
        created_at = int(time.time())
        cur.execute(
            "INSERT INTO chats(id, type, title, created_by, created_at) VALUES(?,?,?,?,?)",
            (chat_id, "dm", dm_key, me, created_at),
        )

    # ensure membership
    cur.execute(
        "INSERT OR IGNORE INTO chat_members(chat_id, username, joined_at) VALUES(?,?,?)",
        (chat_id, me, int(time.time())),
    )
    cur.execute(
        "INSERT OR IGNORE INTO chat_members(chat_id, username, joined_at) VALUES(?,?,?)",
        (chat_id, other, int(time.time())),
    )
    conn.commit()
    conn.close()

    # nice title for UI
    return {"chat": {"id": chat_id, "type": "dm", "title": f"DM: {other}", "created_at": created_at}}


# =========================
# Messages
# =========================
@app.get("/api/messages")
def list_messages(token: str, chat_id: str):
    me = require_user(token)

    conn = db()
    if not is_member(conn, chat_id, me):
        conn.close()
        raise HTTPException(status_code=403, detail="Нет доступа к этому чату.")

    cur = conn.cursor()
    cur.execute(
        "SELECT id, chat_id, sender, text, created_at FROM messages WHERE chat_id=? ORDER BY id DESC LIMIT 100",
        (chat_id,),
    )
    rows = cur.fetchall()
    conn.close()
    msgs = [dict(r) for r in rows][::-1]
    return {"chat_id": chat_id, "messages": msgs}


# =========================
# WebSocket: per chat
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

            # store
            conn = db()
            cur = conn.cursor()
            cur.execute(
                "INSERT INTO messages(chat_id, sender, text, created_at) VALUES(?,?,?,?)",
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
