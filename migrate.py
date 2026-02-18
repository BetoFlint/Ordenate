"""
migrate.py — Migración completa de presupuesto.xlsx → Neon (PostgreSQL).

Ejecutar UNA sola vez:
    python migrate.py

Crea las tablas si no existen y migra todos los datos del Excel.
Si las tablas ya tienen datos, pregunta antes de sobreescribir.
"""

from __future__ import annotations

import sys
import pandas as pd
import psycopg2
import psycopg2.extras
from datetime import date

# ── Connection string ──────────────────────────────────────────────────────────
DB_URL = (
    "postgresql://neondb_owner:npg_gEyhPG0B1HCx"
    "@ep-mute-sea-aintftfq-pooler.c-4.us-east-1.aws.neon.tech"
    "/neondb?sslmode=require&channel_binding=require"
)
EXCEL_FILE = "presupuesto.xlsx"

# ── DDL ────────────────────────────────────────────────────────────────────────
DDL = """
CREATE TABLE IF NOT EXISTS gastos (
    gasto_id            SERIAL PRIMARY KEY,
    nombre              TEXT        NOT NULL,
    categoria           TEXT,
    monto_presupuestado NUMERIC(14,2),
    periodicidad        TEXT,
    fecha_pago          INTEGER,
    fecha_inicio        DATE,
    fecha_termino       DATE
);

CREATE TABLE IF NOT EXISTS pagos (
    pago_id         SERIAL PRIMARY KEY,
    gasto_id        INTEGER REFERENCES gastos(gasto_id) ON DELETE CASCADE,
    monto_real      NUMERIC(14,2),
    fecha_pago_real DATE,
    estado          TEXT
);

CREATE TABLE IF NOT EXISTS ingresos (
    ingreso_id    SERIAL PRIMARY KEY,
    nombre        TEXT        NOT NULL,
    monto         NUMERIC(14,2),
    periodicidad  TEXT,
    fecha_pago    INTEGER,
    fecha_inicio  DATE,
    fecha_termino DATE
);

CREATE TABLE IF NOT EXISTS cuenta (
    id           SERIAL PRIMARY KEY,
    saldo_actual NUMERIC(14,2)
);

CREATE TABLE IF NOT EXISTS gastos_mensuales (
    id                  SERIAL PRIMARY KEY,
    gasto_id            INTEGER REFERENCES gastos(gasto_id) ON DELETE CASCADE,
    year                INTEGER NOT NULL,
    month               INTEGER NOT NULL,
    monto_presupuestado NUMERIC(14,2),
    UNIQUE(gasto_id, year, month)
);

CREATE TABLE IF NOT EXISTS ingresos_mensuales (
    id         SERIAL PRIMARY KEY,
    ingreso_id INTEGER REFERENCES ingresos(ingreso_id) ON DELETE CASCADE,
    year       INTEGER NOT NULL,
    month      INTEGER NOT NULL,
    monto      NUMERIC(14,2),
    UNIQUE(ingreso_id, year, month)
);

CREATE TABLE IF NOT EXISTS comentarios (
    id         SERIAL PRIMARY KEY,
    comentario TEXT
);
"""

# ── Helpers ────────────────────────────────────────────────────────────────────

def _to_date(val) -> date | None:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    if isinstance(val, date):
        return val
    if hasattr(val, "date"):
        return val.date()
    return None


def _to_float(val) -> float | None:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    return float(val)


def _to_int(val) -> int | None:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    return int(val)


# ── Migration functions ────────────────────────────────────────────────────────

def drop_app_tables(cur) -> None:
    """Elimina todas las tablas de la app (excepto users) para recrearlas con el schema correcto."""
    print("  Eliminando tablas antiguas (DROP ... CASCADE)...")
    cur.execute("""
        DROP TABLE IF EXISTS
            comentarios,
            ingresos_mensuales,
            gastos_mensuales,
            cuenta,
            pagos,
            ingresos,
            gastos
        CASCADE;
    """)


def create_tables(cur) -> None:
    print("  Creando tablas con schema correcto...")
    cur.execute(DDL)


def check_existing_data(cur) -> dict[str, int]:
    tables = ["gastos", "pagos", "ingresos", "cuenta",
              "gastos_mensuales", "ingresos_mensuales", "comentarios"]
    counts = {}
    for t in tables:
        try:
            cur.execute(f"SELECT COUNT(*) FROM {t};")
            counts[t] = cur.fetchone()[0]
        except Exception:
            counts[t] = 0
    return counts


def truncate_tables(cur) -> None:
    """Trunca en orden correcto respetando FK."""
    print("  Limpiando tablas existentes...")
    cur.execute("""
        TRUNCATE TABLE comentarios, ingresos_mensuales, gastos_mensuales,
                       cuenta, pagos, ingresos, gastos
        RESTART IDENTITY CASCADE;
    """)


def migrate_gastos(cur, df: pd.DataFrame) -> None:
    print(f"  Migrando gastos ({len(df)} filas)...")
    rows = []
    for _, r in df.iterrows():
        rows.append((
            _to_int(r["gasto_id"]),
            str(r["nombre"]),
            str(r["categoria"]) if not pd.isna(r["categoria"]) else None,
            _to_float(r["monto_presupuestado"]),
            str(r["periodicidad"]) if not pd.isna(r["periodicidad"]) else None,
            _to_int(r["fecha_pago"]),
            _to_date(r["fecha_inicio"]),
            _to_date(r["fecha_termino"]),
        ))
    psycopg2.extras.execute_values(cur, """
        INSERT INTO gastos
            (gasto_id, nombre, categoria, monto_presupuestado,
             periodicidad, fecha_pago, fecha_inicio, fecha_termino)
        VALUES %s
        ON CONFLICT (gasto_id) DO NOTHING;
    """, rows)
    # Sincronizar secuencia SERIAL con el max id migrado
    cur.execute("SELECT setval('gastos_gasto_id_seq', (SELECT MAX(gasto_id) FROM gastos));")


def migrate_pagos(cur, df: pd.DataFrame) -> None:
    print(f"  Migrando pagos ({len(df)} filas)...")
    rows = []
    for _, r in df.iterrows():
        rows.append((
            _to_int(r["pago_id"]),
            _to_int(r["gasto_id"]),
            _to_float(r["monto_real"]),
            _to_date(r["fecha_pago_real"]),
            str(r["estado"]) if not pd.isna(r["estado"]) else None,
        ))
    psycopg2.extras.execute_values(cur, """
        INSERT INTO pagos (pago_id, gasto_id, monto_real, fecha_pago_real, estado)
        VALUES %s
        ON CONFLICT (pago_id) DO NOTHING;
    """, rows)
    cur.execute("SELECT setval('pagos_pago_id_seq', (SELECT MAX(pago_id) FROM pagos));")


def migrate_ingresos(cur, df: pd.DataFrame) -> None:
    print(f"  Migrando ingresos ({len(df)} filas)...")
    rows = []
    for _, r in df.iterrows():
        rows.append((
            _to_int(r["ingreso_id"]),
            str(r["nombre"]),
            _to_float(r["monto"]),
            str(r["periodicidad"]) if not pd.isna(r["periodicidad"]) else None,
            _to_int(r["fecha_pago"]),
            _to_date(r["fecha_inicio"]),
            _to_date(r["fecha_termino"]),
        ))
    psycopg2.extras.execute_values(cur, """
        INSERT INTO ingresos
            (ingreso_id, nombre, monto, periodicidad,
             fecha_pago, fecha_inicio, fecha_termino)
        VALUES %s
        ON CONFLICT (ingreso_id) DO NOTHING;
    """, rows)
    cur.execute("SELECT setval('ingresos_ingreso_id_seq', (SELECT MAX(ingreso_id) FROM ingresos));")


def migrate_cuenta(cur, df: pd.DataFrame) -> None:
    print(f"  Migrando cuenta ({len(df)} filas)...")
    rows = [(float(r["saldo_actual"]),) for _, r in df.iterrows()]
    psycopg2.extras.execute_values(cur,
        "INSERT INTO cuenta (saldo_actual) VALUES %s;",
        rows,
    )


def migrate_gastos_mensuales(cur, df: pd.DataFrame) -> None:
    print(f"  Migrando gastos_mensuales ({len(df)} filas)...")
    rows = []
    for _, r in df.iterrows():
        rows.append((
            _to_int(r["gasto_id"]),
            _to_int(r["year"]),
            _to_int(r["month"]),
            _to_float(r["monto_presupuestado"]),
        ))
    psycopg2.extras.execute_values(cur, """
        INSERT INTO gastos_mensuales (gasto_id, year, month, monto_presupuestado)
        VALUES %s
        ON CONFLICT (gasto_id, year, month) DO NOTHING;
    """, rows)


def migrate_ingresos_mensuales(cur, df: pd.DataFrame) -> None:
    print(f"  Migrando ingresos_mensuales ({len(df)} filas)...")
    rows = []
    for _, r in df.iterrows():
        rows.append((
            _to_int(r["ingreso_id"]),
            _to_int(r["year"]),
            _to_int(r["month"]),
            _to_float(r["monto"]),
        ))
    psycopg2.extras.execute_values(cur, """
        INSERT INTO ingresos_mensuales (ingreso_id, year, month, monto)
        VALUES %s
        ON CONFLICT (ingreso_id, year, month) DO NOTHING;
    """, rows)


def migrate_comentarios(cur, df: pd.DataFrame) -> None:
    print(f"  Migrando comentarios ({len(df)} filas)...")
    rows = [(str(r["comentario"]),) for _, r in df.iterrows() if not pd.isna(r["comentario"])]
    if rows:
        psycopg2.extras.execute_values(cur,
            "INSERT INTO comentarios (comentario) VALUES %s;",
            rows,
        )


# ── Main ───────────────────────────────────────────────────────────────────────

def main() -> None:
    print(f"\n=== Migración presupuesto.xlsx → Neon ===\n")

    # 1. Leer Excel
    print("Leyendo Excel...")
    try:
        dfs = {
            "gastos":              pd.read_excel(EXCEL_FILE, sheet_name="gastos"),
            "pagos":               pd.read_excel(EXCEL_FILE, sheet_name="pagos"),
            "ingresos":            pd.read_excel(EXCEL_FILE, sheet_name="ingresos"),
            "cuenta":              pd.read_excel(EXCEL_FILE, sheet_name="cuenta"),
            "gastos_mensuales":    pd.read_excel(EXCEL_FILE, sheet_name="gastos_mensuales"),
            "ingresos_mensuales":  pd.read_excel(EXCEL_FILE, sheet_name="ingresos_mensuales"),
            "comentarios":         pd.read_excel(EXCEL_FILE, sheet_name="comentarios"),
        }
    except FileNotFoundError:
        print(f"ERROR: No se encontró '{EXCEL_FILE}'. Ejecuta este script desde la carpeta del proyecto.")
        sys.exit(1)

    for name, df in dfs.items():
        print(f"  {name}: {len(df)} filas")

    # 2. Conectar a Neon
    print("\nConectando a Neon...")
    conn = psycopg2.connect(DB_URL)
    conn.autocommit = False
    cur = conn.cursor()

    # 3. Verificar si hay tablas con schema incorrecto o datos previos
    counts = check_existing_data(cur)
    total_existing = sum(counts.values())

    if total_existing > 0:
        print("\nTablas con datos existentes:")
        for t, c in counts.items():
            if c > 0:
                print(f"  {t}: {c} filas")
        resp = input("\n¿Deseas recrear las tablas y migrar desde cero? (s/N): ").strip().lower()
        if resp != "s":
            print("Migración cancelada.")
            cur.close()
            conn.close()
            sys.exit(0)

    # Siempre DROP + CREATE para asegurar schema correcto
    drop_app_tables(cur)
    conn.commit()
    create_tables(cur)
    conn.commit()

    # 5. Migrar en orden (respetando FK)
    print("\nMigrando datos...")
    migrate_gastos(cur, dfs["gastos"])
    migrate_pagos(cur, dfs["pagos"])
    migrate_ingresos(cur, dfs["ingresos"])
    migrate_cuenta(cur, dfs["cuenta"])
    migrate_gastos_mensuales(cur, dfs["gastos_mensuales"])
    migrate_ingresos_mensuales(cur, dfs["ingresos_mensuales"])
    migrate_comentarios(cur, dfs["comentarios"])

    conn.commit()

    # 6. Verificar resultados
    print("\nVerificando resultados finales:")
    final_counts = check_existing_data(cur)
    for t, c in final_counts.items():
        print(f"  {t}: {c} filas")

    cur.close()
    conn.close()
    print("\n✓ Migración completada exitosamente.\n")


if __name__ == "__main__":
    main()
