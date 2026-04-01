#!/usr/bin/env python3
"""
Minimal login proxy server for local development with SQLite.
Handles authentication locally, forwards other requests to remote API if available.
"""
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel
import sqlite3
import hashlib
import secrets
from datetime import datetime, timedelta, timezone

app = FastAPI(title="Transform Mapping - Local Auth Server")

# Database
DB_PATH = "backend/data/trans_fields_mapping.db"

class LoginRequest(BaseModel):
    username: str
    password: str


class ChangePasswordRequest(BaseModel):
    current_password: str
    new_password: str

def simple_hash(password: str) -> str:
    """Simple password hash"""
    return hashlib.sha256(password.encode()).hexdigest()

def verify_password(stored_hash: str, provided_password: str) -> bool:
    """Verify password"""
    return stored_hash == simple_hash(provided_password)


def get_request_token(request: Request) -> str:
    auth_header = str(request.headers.get("Authorization", "")).strip()
    if auth_header.lower().startswith("bearer "):
        return auth_header[7:].strip()
    return str(request.headers.get("X-Auth-Token", "")).strip()


def get_user_role(username: str) -> str:
    return "admin" if StringLower(username) == "admin" else "user"


def StringLower(value: str) -> str:
    return str(value or "").strip().lower()

@app.post("/api/auth/login")
@app.post("/api/login")
async def login(request: LoginRequest):
    """Local login endpoint"""
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        
        # Check user
        cur.execute("SELECT * FROM auth WHERE username = ?", (request.username,))
        user = cur.fetchone()
        conn.close()
        
        if not user:
            raise HTTPException(status_code=401, detail="Invalid username or password")
        
        if user['is_locked']:
            raise HTTPException(status_code=423, detail="User account is locked")
        
        # Verify password
        if not verify_password(user['password_hash'], request.password):
            raise HTTPException(status_code=401, detail="Invalid username or password")
        
        # Generate token
        token = secrets.token_urlsafe(32)
        
        # Store session
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS sessions (
                token TEXT PRIMARY KEY,
                username TEXT,
                created_at TIMESTAMP,
                expires_at TIMESTAMP
            )
        """)
        
        expires_at = datetime.now(timezone.utc) + timedelta(hours=8)
        cur.execute(
            "INSERT INTO sessions (token, username, created_at, expires_at) VALUES (?, ?, ?, ?)",
            (token, request.username, datetime.now(timezone.utc), expires_at)
        )
        conn.commit()
        conn.close()
        
        return {
            "status": "success",
            "session_token": token,
            "token": token,
            "user": {"username": request.username, "role": get_user_role(request.username)},
            "message": "Login successful"
        }
    
    except sqlite3.Error as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

@app.get("/api/auth/me")
@app.get("/api/user")
async def get_user(request: Request):
    """Validate session"""
    token = get_request_token(request)
    
    if not token:
        return JSONResponse(
            status_code=401,
            content={"detail": "Not authenticated"}
        )
    
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        
        cur.execute("""
            SELECT * FROM sessions 
            WHERE token = ? AND expires_at > datetime('now')
        """, (token,))
        
        session = cur.fetchone()
        conn.close()
        
        if not session:
            return JSONResponse(
                status_code=401,
                content={"detail": "Session expired or invalid"}
            )
        
        return {
            "username": session['username'],
            "role": get_user_role(session['username']),
            "authenticated": True
        }
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"detail": str(e)}
        )

@app.post("/api/auth/logout")
@app.post("/api/logout")
async def logout(request: Request):
    """Logout"""
    token = get_request_token(request)
    if token:
        try:
            conn = sqlite3.connect(DB_PATH)
            cur = conn.cursor()
            cur.execute("DELETE FROM sessions WHERE token = ?", (token,))
            conn.commit()
            conn.close()
        except:
            pass
    return {"status": "success"}


@app.post("/api/auth/change-password")
async def change_password(payload: ChangePasswordRequest, request: Request):
    token = get_request_token(request)
    if not token:
        raise HTTPException(status_code=401, detail="Not authenticated")

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute(
        "SELECT username FROM sessions WHERE token = ? AND expires_at > datetime('now')",
        (token,)
    )
    session = cur.fetchone()
    if not session:
        conn.close()
        raise HTTPException(status_code=401, detail="Session expired or invalid")

    username = str(session["username"])
    cur.execute("SELECT password_hash FROM auth WHERE username = ?", (username,))
    user = cur.fetchone()
    if not user or not verify_password(str(user["password_hash"]), payload.current_password):
        conn.close()
        raise HTTPException(status_code=400, detail="Current password is incorrect")

    cur.execute(
        "UPDATE auth SET password_hash = ?, updated_at = CURRENT_TIMESTAMP WHERE username = ?",
        (simple_hash(payload.new_password), username)
    )
    conn.commit()
    conn.close()
    return {"status": "success"}

@app.get("/api/health")
async def health():
    """Health check"""
    return {"status": "ok", "service": "local-auth"}

if __name__ == "__main__":
    import uvicorn
    print("Starting local auth server on http://127.0.0.1:8000")
    print(f"Database: {DB_PATH}")
    print("\nDefault credentials:")
    print("  Username: admin")
    print("  Password: admin")
    uvicorn.run(app, host="127.0.0.1", port=8000, log_level="info")
