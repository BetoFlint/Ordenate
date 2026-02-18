"""
neon_data.py — Capa de acceso a datos sobre Neon (PostgreSQL).

Reemplaza la lectura/escritura del archivo presupuesto.xlsx.

API pública:
    load_data()          → dict[str, pd.DataFrame]
    save_data(data)      → None
"""

from __future__ import annotations

from datetime import date, datetime

import pandas as pd
import psycopg2.extras
import streamlit as st
from sqlalchemy import create_engine, text

from db import get_connection
from logger import log_time


def _get_engine():
    """Devuelve un SQLAlchemy engine usando el URL de st.secrets."""
    url = st.secrets["database"]["url"]
    sa_url = url.replace("postgresql://", "postgresql+psycopg2://", 1)
    return create_engine(sa_url, pool_pre_ping=True)


# ── Helpers de conversión ───────────────────────────────────────────────────

def _to_pg_date(val) -> date | None:
    """Convierte cualquier valor de fecha a datetime.date, o None si es nulo."""
    if val is None:
        return None
    if isinstance(val, float):
        import math
        if math.isnan(val):
            return None
    try:
        if pd.isna(val):
            return None
    except (TypeError, ValueError):
        pass
    if isinstance(val, datetime):
        return val.date()
    if isinstance(val, date):
        return val
    if hasattr(val, "date"):           # pd.Timestamp
        return val.date()
    return None


def _to_float(val) -> float | None:
    try:
        if pd.isna(val):
            return None
    except (TypeError, ValueError):
        pass
    return float(val) if val is not None else None


def _to_int(val) -> int | None:
    try:
        if pd.isna(val):
            return None
    except (TypeError, ValueError):
        pass
    return int(val) if val is not None else None


def _to_str(val) -> str | None:
    try:
        if pd.isna(val):
            return None
    except (TypeError, ValueError):
        pass
    return str(val) if val is not None else None


# ── LOAD ────────────────────────────────────────────────────────────────────

@log_time
def load_data(user_id: int) -> dict:
    """Lee las tablas de Neon filtradas por user_id y las devuelve como DataFrames."""
    engine = _get_engine()
    uid = int(user_id)
    with engine.connect() as conn:
        gastos = pd.read_sql(
            "SELECT gasto_id, nombre, categoria, monto_presupuestado, "
            "       periodicidad, fecha_pago, fecha_inicio, fecha_termino "
            "FROM gastos WHERE user_id = %(uid)s ORDER BY gasto_id",
            conn, params={"uid": uid},
        )
        pagos = pd.read_sql(
            "SELECT p.pago_id, p.gasto_id, p.monto_real, p.fecha_pago_real, p.estado "
            "FROM pagos p "
            "JOIN gastos g ON p.gasto_id = g.gasto_id "
            "WHERE g.user_id = %(uid)s ORDER BY p.pago_id",
            conn, params={"uid": uid},
        )
        ingresos = pd.read_sql(
            "SELECT ingreso_id, nombre, monto, periodicidad, "
            "       fecha_pago, fecha_inicio, fecha_termino "
            "FROM ingresos WHERE user_id = %(uid)s ORDER BY ingreso_id",
            conn, params={"uid": uid},
        )
        cuenta = pd.read_sql(
            "SELECT saldo_actual FROM cuenta WHERE user_id = %(uid)s LIMIT 1",
            conn, params={"uid": uid},
        )
        gastos_mensuales = pd.read_sql(
            "SELECT gm.gasto_id, gm.year, gm.month, gm.monto_presupuestado "
            "FROM gastos_mensuales gm "
            "JOIN gastos g ON gm.gasto_id = g.gasto_id "
            "WHERE g.user_id = %(uid)s ORDER BY gm.gasto_id, gm.year, gm.month",
            conn, params={"uid": uid},
        )
        ingresos_mensuales = pd.read_sql(
            "SELECT im.ingreso_id, im.year, im.month, im.monto "
            "FROM ingresos_mensuales im "
            "JOIN ingresos i ON im.ingreso_id = i.ingreso_id "
            "WHERE i.user_id = %(uid)s ORDER BY im.ingreso_id, im.year, im.month",
            conn, params={"uid": uid},
        )
        comentarios = pd.read_sql(
            "SELECT comentario FROM comentarios WHERE user_id = %(uid)s",
            conn, params={"uid": uid},
        )

    # Asegurar tipos int en columnas de id
    for df, col in [
        (gastos, "gasto_id"),
        (pagos, "pago_id"),
        (pagos, "gasto_id"),
        (ingresos, "ingreso_id"),
        (gastos_mensuales, "gasto_id"),
        (ingresos_mensuales, "ingreso_id"),
    ]:
        if col in df.columns:
            df[col] = df[col].astype("Int64")

    # Si cuenta está vacía, inicializarla con saldo 0
    if cuenta.empty:
        cuenta = pd.DataFrame({"saldo_actual": [0]})

    return {
        "gastos": gastos,
        "pagos": pagos,
        "ingresos": ingresos,
        "cuenta": cuenta,
        "gastos_mensuales": gastos_mensuales,
        "ingresos_mensuales": ingresos_mensuales,
        "comentarios": comentarios,
    }


# ── SAVE ────────────────────────────────────────────────────────────────────

@log_time
def save_data(data: dict, user_id: int) -> None:
    """Sincroniza el dict de DataFrames hacia Neon para el user_id dado.

    Estrategia: DELETE WHERE user_id = X (en orden hijo→padre)
    y luego INSERT con user_id incluido (en orden padre→hijo).
    """
    uid = int(user_id)
    gastos_df             = data.get("gastos",             pd.DataFrame())
    pagos_df              = data.get("pagos",              pd.DataFrame())
    ingresos_df           = data.get("ingresos",           pd.DataFrame())
    cuenta_df             = data.get("cuenta",             pd.DataFrame())
    gastos_mensuales_df   = data.get("gastos_mensuales",   pd.DataFrame())
    ingresos_mensuales_df = data.get("ingresos_mensuales", pd.DataFrame())
    comentarios_df        = data.get("comentarios",        pd.DataFrame())

    conn = get_connection()
    conn.autocommit = False
    cur = conn.cursor()

    try:
        # 1. Borrar solo los datos del usuario (orden hijo → padre)
        cur.execute("DELETE FROM comentarios WHERE user_id = %s;", (uid,))
        cur.execute(
            "DELETE FROM ingresos_mensuales im "
            "USING ingresos i WHERE im.ingreso_id = i.ingreso_id AND i.user_id = %s;",
            (uid,),
        )
        cur.execute(
            "DELETE FROM gastos_mensuales gm "
            "USING gastos g WHERE gm.gasto_id = g.gasto_id AND g.user_id = %s;",
            (uid,),
        )
        cur.execute("DELETE FROM cuenta WHERE user_id = %s;", (uid,))
        cur.execute(
            "DELETE FROM pagos p "
            "USING gastos g WHERE p.gasto_id = g.gasto_id AND g.user_id = %s;",
            (uid,),
        )
        cur.execute("DELETE FROM ingresos WHERE user_id = %s;", (uid,))
        cur.execute("DELETE FROM gastos WHERE user_id = %s;", (uid,))

        # 2. Insertar con user_id (orden padre → hijo)

        # gastos
        if not gastos_df.empty:
            gastos_rows = [
                (
                    _to_int(r["gasto_id"]),
                    _to_str(r["nombre"]),
                    _to_str(r.get("categoria")),
                    _to_float(r.get("monto_presupuestado")),
                    _to_str(r.get("periodicidad")),
                    _to_int(r.get("fecha_pago")),
                    _to_pg_date(r.get("fecha_inicio")),
                    _to_pg_date(r.get("fecha_termino")),
                    uid,
                )
                for _, r in gastos_df.iterrows()
            ]
            psycopg2.extras.execute_values(cur, """
                INSERT INTO gastos
                    (gasto_id, nombre, categoria, monto_presupuestado,
                     periodicidad, fecha_pago, fecha_inicio, fecha_termino, user_id)
                VALUES %s;
            """, gastos_rows)
            cur.execute(
                "SELECT setval('gastos_gasto_id_seq', "
                "(SELECT COALESCE(MAX(gasto_id), 0) FROM gastos));"
            )

        # ingresos
        if not ingresos_df.empty:
            ingresos_rows = [
                (
                    _to_int(r["ingreso_id"]),
                    _to_str(r["nombre"]),
                    _to_float(r.get("monto")),
                    _to_str(r.get("periodicidad")),
                    _to_int(r.get("fecha_pago")),
                    _to_pg_date(r.get("fecha_inicio")),
                    _to_pg_date(r.get("fecha_termino")),
                    uid,
                )
                for _, r in ingresos_df.iterrows()
            ]
            psycopg2.extras.execute_values(cur, """
                INSERT INTO ingresos
                    (ingreso_id, nombre, monto, periodicidad,
                     fecha_pago, fecha_inicio, fecha_termino, user_id)
                VALUES %s;
            """, ingresos_rows)
            cur.execute(
                "SELECT setval('ingresos_ingreso_id_seq', "
                "(SELECT COALESCE(MAX(ingreso_id), 0) FROM ingresos));"
            )

        # pagos (sin user_id propio, hereda via gasto_id → gastos.user_id)
        if not pagos_df.empty:
            pagos_rows = [
                (
                    _to_int(r["pago_id"]),
                    _to_int(r["gasto_id"]),
                    _to_float(r.get("monto_real")),
                    _to_pg_date(r.get("fecha_pago_real")),
                    _to_str(r.get("estado")),
                )
                for _, r in pagos_df.iterrows()
            ]
            psycopg2.extras.execute_values(cur, """
                INSERT INTO pagos (pago_id, gasto_id, monto_real, fecha_pago_real, estado)
                VALUES %s;
            """, pagos_rows)
            cur.execute(
                "SELECT setval('pagos_pago_id_seq', "
                "(SELECT COALESCE(MAX(pago_id), 0) FROM pagos));"
            )

        # cuenta
        if not cuenta_df.empty:
            saldo = _to_float(cuenta_df.iloc[0]["saldo_actual"])
            cur.execute("INSERT INTO cuenta (saldo_actual, user_id) VALUES (%s, %s);", (saldo, uid))

        # gastos_mensuales (sin user_id propio, hereda via gasto_id → gastos.user_id)
        if not gastos_mensuales_df.empty:
            gm_rows = [
                (
                    _to_int(r["gasto_id"]),
                    _to_int(r["year"]),
                    _to_int(r["month"]),
                    _to_float(r.get("monto_presupuestado")),
                )
                for _, r in gastos_mensuales_df.iterrows()
            ]
            psycopg2.extras.execute_values(cur, """
                INSERT INTO gastos_mensuales (gasto_id, year, month, monto_presupuestado)
                VALUES %s;
            """, gm_rows)

        # ingresos_mensuales (sin user_id propio, hereda via ingreso_id → ingresos.user_id)
        if not ingresos_mensuales_df.empty:
            im_rows = [
                (
                    _to_int(r["ingreso_id"]),
                    _to_int(r["year"]),
                    _to_int(r["month"]),
                    _to_float(r.get("monto")),
                )
                for _, r in ingresos_mensuales_df.iterrows()
            ]
            psycopg2.extras.execute_values(cur, """
                INSERT INTO ingresos_mensuales (ingreso_id, year, month, monto)
                VALUES %s;
            """, im_rows)

        # comentarios
        if not comentarios_df.empty:
            com_rows = [
                (_to_str(r.get("comentario")), uid)
                for _, r in comentarios_df.iterrows()
                if _to_str(r.get("comentario")) is not None
            ]
            if com_rows:
                psycopg2.extras.execute_values(cur, """
                    INSERT INTO comentarios (comentario, user_id) VALUES %s;
                """, com_rows)

        conn.commit()

    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()
