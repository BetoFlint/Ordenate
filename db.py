"""
db.py – Conexión a la base de datos Neon (PostgreSQL).

Provee:
  - get_connection()        → devuelve una conexión psycopg2 lista para usar
  - init_db()               → crea la tabla 'users' si no existe
  - add_user_id_columns()   → añade user_id a las tablas de datos si no existe
"""

from __future__ import annotations

import psycopg2
import psycopg2.extras
import streamlit as st

from logger import log_time


def _get_db_url() -> str:
    """Lee la URL de conexión desde st.secrets (archivo .streamlit/secrets.toml)."""
    return st.secrets["database"]["url"]


def get_connection() -> psycopg2.extensions.connection:
    """Abre y devuelve una nueva conexión a Neon.

    El caller es responsable de cerrarla (usar con 'with' o .close()).
    """
    url = _get_db_url()
    conn = psycopg2.connect(url)
    return conn


@log_time
def init_db() -> None:
    """Crea la tabla 'users' en Neon si aún no existe."""
    ddl = """
        CREATE TABLE IF NOT EXISTS users (
            id         SERIAL PRIMARY KEY,
            username   TEXT UNIQUE NOT NULL,
            password   TEXT NOT NULL,
            created_at TIMESTAMPTZ DEFAULT now()
        );
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(ddl)
        conn.commit()


@log_time
def add_user_id_columns() -> None:
    """Añade la columna user_id a las tablas de datos si aún no existe.

    - gastos, ingresos, cuenta, comentarios reciben user_id directo.
    - pagos, gastos_mensuales, ingresos_mensuales heredan el filtro via FK.
    Los registros existentes sin user_id quedan asignados al usuario id=1 (admin).
    """
    statements = [
        # Añadir columna si no existe (idempotente)
        "ALTER TABLE gastos     ADD COLUMN IF NOT EXISTS user_id INTEGER REFERENCES users(id);",
        "ALTER TABLE ingresos   ADD COLUMN IF NOT EXISTS user_id INTEGER REFERENCES users(id);",
        "ALTER TABLE cuenta     ADD COLUMN IF NOT EXISTS user_id INTEGER REFERENCES users(id);",
        "ALTER TABLE comentarios ADD COLUMN IF NOT EXISTS user_id INTEGER REFERENCES users(id);",
        # Asignar registros huérfanos al primer usuario (admin)
        "UPDATE gastos      SET user_id = 1 WHERE user_id IS NULL;",
        "UPDATE ingresos    SET user_id = 1 WHERE user_id IS NULL;",
        "UPDATE cuenta      SET user_id = 1 WHERE user_id IS NULL;",
        "UPDATE comentarios SET user_id = 1 WHERE user_id IS NULL;",
    ]
    with get_connection() as conn:
        with conn.cursor() as cur:
            for stmt in statements:
                cur.execute(stmt)
        conn.commit()
