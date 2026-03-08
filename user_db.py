"""
user_db.py — User and Favorites SQLite database management.
"""

import sqlite3
import threading
from pathlib import Path

DB_PATH = Path("./storage/users.db")

# Use a thread-local for SQLite connection
_local = threading.local()

def get_db() -> sqlite3.Connection:
    if not hasattr(_local, "con"):
        DB_PATH.parent.mkdir(parents=True, exist_ok=True)
        _local.con = sqlite3.connect(DB_PATH)
        _local.con.row_factory = sqlite3.Row
        _init_db(_local.con)
    return _local.con

def _init_db(con: sqlite3.Connection):
    con.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id TEXT PRIMARY KEY,
            email TEXT,
            name TEXT,
            picture_url TEXT
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS favorites (
            user_id TEXT,
            sticker_id TEXT,
            PRIMARY KEY (user_id, sticker_id),
            FOREIGN KEY(user_id) REFERENCES users(id)
        )
    """)
    con.commit()

def upsert_user(user_id: str, email: str, name: str, picture_url: str):
    con = get_db()
    con.execute("""
        INSERT INTO users (id, email, name, picture_url)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
            email=excluded.email,
            name=excluded.name,
            picture_url=excluded.picture_url
    """, (user_id, email, name, picture_url))
    con.commit()

def add_favorite(user_id: str, sticker_id: str):
    con = get_db()
    con.execute("""
        INSERT OR IGNORE INTO favorites (user_id, sticker_id)
        VALUES (?, ?)
    """, (user_id, sticker_id))
    con.commit()

def remove_favorite(user_id: str, sticker_id: str):
    con = get_db()
    con.execute("""
        DELETE FROM favorites WHERE user_id = ? AND sticker_id = ?
    """, (user_id, sticker_id))
    con.commit()

def get_favorites(user_id: str) -> list[str]:
    con = get_db()
    cur = con.execute("""
        SELECT sticker_id FROM favorites WHERE user_id = ?
    """, (user_id,))
    return [row["sticker_id"] for row in cur.fetchall()]
