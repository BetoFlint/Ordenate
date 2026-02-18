"""
auth.py – Lógica de autenticación contra la tabla 'users' en Neon.

Funciones públicas:
  - verify_login(username, password)  → int | None  (user_id si OK, None si falla)
  - create_user(username, password)   → None
  - user_exists(username)             → bool
  - list_users()                      → list[dict]  (id, username, created_at)
"""

from __future__ import annotations

import bcrypt

from db import get_connection
from logger import log_time


def _hash_password(plain: str) -> str:
    """Genera un hash bcrypt de la contraseña en texto plano."""
    return bcrypt.hashpw(plain.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")


def _check_password(plain: str, hashed: str) -> bool:
    """Verifica que la contraseña en texto plano coincida con el hash almacenado."""
    return bcrypt.checkpw(plain.encode("utf-8"), hashed.encode("utf-8"))


@log_time
def verify_login(username: str, password: str) -> int | None:
    """Verifica credenciales y devuelve el user_id si son correctas, None si no."""
    sql = "SELECT id, password FROM users WHERE username = %s LIMIT 1;"
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (username.strip(),))
                row = cur.fetchone()
    except Exception:
        return None

    if row is None:
        return None

    user_id: int = row[0]
    stored_hash: str = row[1]
    return user_id if _check_password(password, stored_hash) else None


@log_time
def create_user(username: str, password: str) -> None:
    """Inserta un nuevo usuario con contraseña hasheada.

    Lanza psycopg2.errors.UniqueViolation si el usuario ya existe.
    """
    hashed = _hash_password(password)
    sql = "INSERT INTO users (username, password) VALUES (%s, %s);"
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (username.strip(), hashed))
        conn.commit()


def user_exists(username: str) -> bool:
    """Devuelve True si el username ya está registrado en la tabla users."""
    sql = "SELECT 1 FROM users WHERE username = %s LIMIT 1;"
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (username.strip(),))
                return cur.fetchone() is not None
    except Exception:
        return False


def list_users() -> list[dict]:
    """Devuelve todos los usuarios (id, username, created_at), sin contraseñas."""
    sql = "SELECT id, username, created_at FROM users ORDER BY id;"
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
                rows = cur.fetchall()
        return [{"id": r[0], "username": r[1], "created_at": r[2]} for r in rows]
    except Exception:
        return []
