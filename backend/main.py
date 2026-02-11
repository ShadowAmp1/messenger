from __future__ import annotations

import os
import time
import json
import sqlite3
import hashlib
import hmac
import secrets
from typing import Dict, Set, Optional

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

DB_PATH = os.path.join(os.path.dirname(__file__), "app.db")
JWT_SECRET = os.environ.get("JWT_SECRET", "dev-secret-change-me")
JWT_TTL_SECONDS = 60 * 60 * 24 * 7  # 7 days


# ---------- DB ----------
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


# ---------- Password hashing ----------
def hash_password(password: str, salt: Optional[str] = None) -> str:
    if salt is None:
        salt = secrets.token_hex(16)
    dk = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt.encode("utf-8"), 200_000)
    return f"pbkdf2_sha256$200000${salt}${dk.hex()}"


def verify_password(password: str, stored: str) -> bool:
    try:
        _, _, salt, hexhash = stored.split("$", 3)
    except ValueError:
        return False
    test = hash_password(password, salt)
    return hmac.compare_digest(test, stored)


# ---------- Tiny JWT (HS256) ----------
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
    if payload.get("exp", 0) < int(time.time()):
        raise HTTPException(status_code=401, detail="Token expired")
    return payload


# ---------- FastAPI ----------
init_db()
app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Serve frontend
FRONTEND_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "frontend"))
app.mount("/", StaticFiles(directory=FRONTEND_DIR, html=True), name="frontend")


class AuthIn(BaseModel):
    username: str
    password: str


class MessageIn(BaseModel):
    text: str


CHAT_ID = "general"  # one room for MVP


# Active connections per chat
connections: Dict[str, Set[WebSocket]] = {CHAT_ID: set()}


import re

USERNAME_RE = re.compile(r"^[a-zA-Z0-9_]{3,20}$")

@app.post("/api/register")
def register(data: AuthIn):
    username = data.username.strip()
    password = data.password

    if not USERNAME_RE.match(username):
        raise HTTPException(
            status_code=400,
            detail="Username: 3-20 символов, только буквы/цифры/_."
        )
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
    except sqlite3.IntegrityError:
        raise HTTPException(status_code=400, detail="Такой username уже занят.")
    finally:
        conn.close()
    return {"ok": True}



@app.post("/api/login")
def login(data: AuthIn):
    conn = db()
    cur = conn.cursor()
    cur.execute("SELECT username, pass_hash FROM users WHERE username = ?", (data.username.strip(),))
    row = cur.fetchone()
    conn.close()
    if not row or not verify_password(data.password, row["pass_hash"]):
        raise HTTPException(status_code=401, detail="Invalid credentials")

    now = int(time.time())
    token = jwt_sign({"sub": row["username"], "iat": now, "exp": now + JWT_TTL_SECONDS})
    return {"token": token, "username": row["username"]}


@app.get("/api/messages")
def list_messages(token: str):
    payload = jwt_verify(token)
    _ = payload["sub"]

    conn = db()
    cur = conn.cursor()
    cur.execute(
        "SELECT id, chat_id, sender, text, created_at FROM messages WHERE chat_id=? ORDER BY id DESC LIMIT 100",
        (CHAT_ID,),
    )
    rows = cur.fetchall()
    conn.close()
    # return oldest -> newest
    msgs = [dict(r) for r in rows][::-1]
    return {"chat_id": CHAT_ID, "messages": msgs}


@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):
    token = ws.query_params.get("token")
    if not token:
        await ws.close(code=4401)
        return
    try:
        payload = jwt_verify(token)
    except HTTPException:
        await ws.close(code=4401)
        return

    username = payload["sub"]
    await ws.accept()

    connections[CHAT_ID].add(ws)

    # notify presence (simple)
    join_event = {"type": "presence", "user": username, "status": "online", "ts": int(time.time())}
    await broadcast(CHAT_ID, join_event)

    try:
        while True:
            raw = await ws.receive_text()
            data = json.loads(raw)

            if data.get("type") == "message":
                text = (data.get("text") or "").strip()
                if not text:
                    continue

                ts = int(time.time())
                # store
                conn = db()
                cur = conn.cursor()
                cur.execute(
                    "INSERT INTO messages(chat_id, sender, text, created_at) VALUES(?,?,?,?)",
                    (CHAT_ID, username, text, ts),
                )
                conn.commit()
                msg_id = cur.lastrowid
                conn.close()

                event = {
                    "type": "message",
                    "id": msg_id,
                    "chat_id": CHAT_ID,
                    "sender": username,
                    "text": text,
                    "created_at": ts,
                }
                await broadcast(CHAT_ID, event)

    except WebSocketDisconnect:
        pass
    finally:
        connections[CHAT_ID].discard(ws)
        leave_event = {"type": "presence", "user": username, "status": "offline", "ts": int(time.time())}
        await broadcast(CHAT_ID, leave_event)


async def broadcast(chat_id: str, event: dict) -> None:
    dead = []
    for c in list(connections.get(chat_id, set())):
        try:
            await c.send_text(json.dumps(event))
        except Exception:
            dead.append(c)
    for d in dead:
        connections[chat_id].discard(d)
