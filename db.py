"""
db.py – Conexión a la base de datos Neon (PostgreSQL).

Provee:
  - get_connection()        → devuelve una conexión psycopg2 lista para usar
  - init_db()               → crea la tabla 'users' si no existe
  - add_user_id_columns()   → añade user_id a las tablas de datos si no existe
"""

from __future__ import annotations

import os

import psycopg2
import psycopg2.extras
import streamlit as st

from logger import log_time


def _get_db_url() -> str:
    """Lee la URL de conexión.

    Prioridad:
      1. Variable de entorno DATABASE_URL  (Render / producción)
      2. st.secrets["database"]["url"]     (desarrollo local)
    """
    env_url = os.environ.get("DATABASE_URL")
    if env_url:
        return env_url
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
            first_name TEXT,
            last_name_paterno TEXT,
            last_name_materno TEXT,
            email      TEXT,
            created_at TIMESTAMPTZ DEFAULT now()
        );
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(ddl)
            # En instalaciones previas, estas columnas pueden no existir.
            cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS first_name TEXT;")
            cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS last_name_paterno TEXT;")
            cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS last_name_materno TEXT;")
            cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS email TEXT;")
            cur.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS users_email_unique_idx "
                "ON users (email) WHERE email IS NOT NULL;"
            )
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
        "ALTER TABLE cuenta               ADD COLUMN IF NOT EXISTS user_id INTEGER REFERENCES users(id);",
        "ALTER TABLE comentarios          ADD COLUMN IF NOT EXISTS user_id INTEGER REFERENCES users(id);",
        "ALTER TABLE pagos                ADD COLUMN IF NOT EXISTS user_id INTEGER REFERENCES users(id);",
        "ALTER TABLE gastos_mensuales     ADD COLUMN IF NOT EXISTS user_id INTEGER REFERENCES users(id);",
        "ALTER TABLE ingresos_mensuales   ADD COLUMN IF NOT EXISTS user_id INTEGER REFERENCES users(id);",
        # Columnas cifradas para montos sensibles
        "ALTER TABLE pagos                ADD COLUMN IF NOT EXISTS monto_real_enc TEXT;",
        "ALTER TABLE gastos_mensuales     ADD COLUMN IF NOT EXISTS monto_presupuestado_enc TEXT;",
        "ALTER TABLE ingresos_mensuales   ADD COLUMN IF NOT EXISTS monto_enc TEXT;",
        # Asignar registros huérfanos al primer usuario (admin)
        "UPDATE cuenta               SET user_id = 1 WHERE user_id IS NULL;",
        "UPDATE comentarios          SET user_id = 1 WHERE user_id IS NULL;",
        "UPDATE pagos                SET user_id = 1 WHERE user_id IS NULL;",
        "UPDATE gastos_mensuales     SET user_id = 1 WHERE user_id IS NULL;",
        "UPDATE ingresos_mensuales   SET user_id = 1 WHERE user_id IS NULL;",
                # Eliminar restricciones únicas globales antiguas (rompen multiusuario)
                "ALTER TABLE gastos_mensuales DROP CONSTRAINT IF EXISTS gastos_mensuales_gasto_id_year_month_key;",
                "ALTER TABLE ingresos_mensuales DROP CONSTRAINT IF EXISTS ingresos_mensuales_ingreso_id_year_month_key;",
                # Deduplicar por usuario antes de crear nuevos índices únicos
                """
                DELETE FROM gastos_mensuales gm
                USING gastos_mensuales gm2
                WHERE gm.id > gm2.id
                    AND COALESCE(gm.user_id, 1) = COALESCE(gm2.user_id, 1)
                    AND gm.gasto_id = gm2.gasto_id
                    AND gm.year = gm2.year
                    AND gm.month = gm2.month;
                """,
                """
                DELETE FROM ingresos_mensuales im
                USING ingresos_mensuales im2
                WHERE im.id > im2.id
                    AND COALESCE(im.user_id, 1) = COALESCE(im2.user_id, 1)
                    AND im.ingreso_id = im2.ingreso_id
                    AND im.year = im2.year
                    AND im.month = im2.month;
                """,
                # Nueva unicidad por usuario
                """
                CREATE UNIQUE INDEX IF NOT EXISTS ux_gastos_mensuales_user_gasto_year_month
                ON gastos_mensuales (user_id, gasto_id, year, month);
                """,
                """
                CREATE UNIQUE INDEX IF NOT EXISTS ux_ingresos_mensuales_user_ingreso_year_month
                ON ingresos_mensuales (user_id, ingreso_id, year, month);
                """,
    ]
    with get_connection() as conn:
        with conn.cursor() as cur:
            for stmt in statements:
                cur.execute(stmt)
        conn.commit()
