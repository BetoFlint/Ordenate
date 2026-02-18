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
def load_data() -> dict:
    """Lee todas las tablas de Neon y las devuelve como DataFrames.

    El dict resultante tiene las mismas claves y estructura de columnas
    que el antiguo presupuesto.xlsx.
    """
    engine = _get_engine()
    with engine.connect() as conn:
        gastos = pd.read_sql(
            "SELECT gasto_id, nombre, categoria, monto_presupuestado, "
            "       periodicidad, fecha_pago, fecha_inicio, fecha_termino "
            "FROM gastos ORDER BY gasto_id",
            conn,
        )
        pagos = pd.read_sql(
            "SELECT pago_id, gasto_id, monto_real, fecha_pago_real, estado "
            "FROM pagos ORDER BY pago_id",
            conn,
        )
        ingresos = pd.read_sql(
            "SELECT ingreso_id, nombre, monto, periodicidad, "
            "       fecha_pago, fecha_inicio, fecha_termino "
            "FROM ingresos ORDER BY ingreso_id",
            conn,
        )
        cuenta = pd.read_sql(
            "SELECT saldo_actual FROM cuenta LIMIT 1",
            conn,
        )
        gastos_mensuales = pd.read_sql(
            "SELECT gasto_id, year, month, monto_presupuestado "
            "FROM gastos_mensuales ORDER BY gasto_id, year, month",
            conn,
        )
        ingresos_mensuales = pd.read_sql(
            "SELECT ingreso_id, year, month, monto "
            "FROM ingresos_mensuales ORDER BY ingreso_id, year, month",
            conn,
        )
        comentarios = pd.read_sql(
            "SELECT comentario FROM comentarios",
            conn,
        )

    # Asegurar tipos correctos en columnas de id para evitar problemas de
    # comparación int64 vs object más adelante en la app.
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
def save_data(data: dict) -> None:
    """Sincroniza el dict de DataFrames hacia Neon en una única transacción.

    Estrategia: DELETE todas las filas de cada tabla (en orden que respeta FK)
    y luego INSERT el estado completo desde los DataFrames.
    """
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
        # 1. Borrar en orden hijo → padre
        cur.execute("""
            DELETE FROM comentarios;
            DELETE FROM ingresos_mensuales;
            DELETE FROM gastos_mensuales;
            DELETE FROM cuenta;
            DELETE FROM pagos;
            DELETE FROM ingresos;
            DELETE FROM gastos;
        """)

        # 2. Insertar en orden padre → hijo

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
                )
                for _, r in gastos_df.iterrows()
            ]
            psycopg2.extras.execute_values(cur, """
                INSERT INTO gastos
                    (gasto_id, nombre, categoria, monto_presupuestado,
                     periodicidad, fecha_pago, fecha_inicio, fecha_termino)
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
                )
                for _, r in ingresos_df.iterrows()
            ]
            psycopg2.extras.execute_values(cur, """
                INSERT INTO ingresos
                    (ingreso_id, nombre, monto, periodicidad,
                     fecha_pago, fecha_inicio, fecha_termino)
                VALUES %s;
            """, ingresos_rows)
            cur.execute(
                "SELECT setval('ingresos_ingreso_id_seq', "
                "(SELECT COALESCE(MAX(ingreso_id), 0) FROM ingresos));"
            )

        # pagos
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
            cur.execute("INSERT INTO cuenta (saldo_actual) VALUES (%s);", (saldo,))

        # gastos_mensuales
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

        # ingresos_mensuales
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
                (_to_str(r.get("comentario")),)
                for _, r in comentarios_df.iterrows()
                if _to_str(r.get("comentario")) is not None
            ]
            if com_rows:
                psycopg2.extras.execute_values(cur, """
                    INSERT INTO comentarios (comentario) VALUES %s;
                """, com_rows)

        conn.commit()

    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()
