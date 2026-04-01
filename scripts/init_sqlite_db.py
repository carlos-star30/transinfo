#!/usr/bin/env python3
"""
Initialize SQLite database with required schema for the transformation mapping app.
This allows running without MySQL.
"""
import sqlite3
import os
from datetime import datetime
from pathlib import Path

DB_DIR = Path("backend/data")
DB_DIR.mkdir(parents=True, exist_ok=True)
DB_PATH = DB_DIR / "trans_fields_mapping.db"

# Remove existing DB if needed
if DB_PATH.exists():
    print(f"⚠ Removing existing database: {DB_PATH}")
    DB_PATH.unlink()

conn = sqlite3.connect(str(DB_PATH))
cur = conn.cursor()

# Create auth table for login support
cur.execute("""
CREATE TABLE auth (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    username TEXT NOT NULL UNIQUE,
    password_hash TEXT NOT NULL,
    is_locked INTEGER DEFAULT 0,
    failed_attempts INTEGER DEFAULT 0,
    temp_lock_until TIMESTAMP DEFAULT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
""")

# Create status table for import tracking
cur.execute("""
CREATE TABLE status (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_name TEXT NOT NULL,
    source_type TEXT DEFAULT 'UNKNOWN',
    source_system TEXT,
    record_count INTEGER DEFAULT 0,
    import_status TEXT DEFAULT 'PENDING',
    import_time TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (source_name, source_system)
)
""")

# Create transformation mapping table
cur.execute("""
CREATE TABLE transformations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_name TEXT NOT NULL,
    source_system TEXT,
    target_name TEXT NOT NULL,
    field_mapping TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
""")

# Create simple auth entry for admin
# Password hash for "admin" using a simple method (in production use proper hashing)
import hashlib
import binascii

def simple_hash(password: str) -> str:
    """Simple password hash for demo purposes"""
    return binascii.hexlify(hashlib.sha256(password.encode()).digest()).decode()

admin_hash = simple_hash("admin")

cur.execute(
    "INSERT INTO auth (username, password_hash, is_locked) VALUES (?, ?, ?)",
    ("admin", admin_hash, 0)
)

conn.commit()
conn.close()

print(f"✓ SQLite database initialized: {DB_PATH}")
print(f"  - Tables created: auth, status, transformations")
print(f"  - Default admin user created (username: admin, password: admin)")
print(f"\nTo start with SQLite, run:")
print(f'  $env:DB_TYPE="sqlite"')
print(f'  $env:SQLITE_DB_PATH="{DB_PATH.as_posix()}"')
print(f'  C:/python -m uvicorn backend.import_status_api:app --port 8000')
