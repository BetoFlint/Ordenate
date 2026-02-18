"""
db.py – Conexión a la base de datos Neon (PostgreSQL).

Provee:
  - get_connection()  → devuelve una conexión psycopg2 lista para usar
  - init_db()         → crea la tabla 'users' si no existe (ejecutar una vez al iniciar)
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
    """Crea la tabla 'users' en Neon si aún no existe.

    Esquema:
        id        SERIAL PRIMARY KEY
        username  TEXT UNIQUE NOT NULL
        password  TEXT NOT NULL          -- bcrypt hash
        created_at TIMESTAMPTZ DEFAULT now()
    """
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
