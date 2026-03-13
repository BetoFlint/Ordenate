"""
migrate_encrypt_amounts.py

Cifra montos historicos en BD para reforzar privacidad:
- pagos.monto_real -> pagos.monto_real_enc
- gastos_mensuales.monto_presupuestado -> gastos_mensuales.monto_presupuestado_enc
- ingresos_mensuales.monto -> ingresos_mensuales.monto_enc

Uso:
  python migrate_encrypt_amounts.py
  python migrate_encrypt_amounts.py --dry-run
  python migrate_encrypt_amounts.py --keep-plain

Notas:
- Por defecto, luego de cifrar, limpia la columna numerica en claro (la deja en NULL).
- Usa DATA_ENCRYPTION_KEY (o fallback por SESSION_SECRET) via data_crypto.py.
"""

from __future__ import annotations

import argparse
from typing import Any

import psycopg2.extras

from db import get_connection
from data_crypto import encrypt_amount


TABLE_CONFIG = [
    {
        "table": "pagos",
        "id_col": "pago_id",
        "plain_col": "monto_real",
        "enc_col": "monto_real_enc",
    },
    {
        "table": "gastos_mensuales",
        "id_col": "id",
        "plain_col": "monto_presupuestado",
        "enc_col": "monto_presupuestado_enc",
    },
    {
        "table": "ingresos_mensuales",
        "id_col": "id",
        "plain_col": "monto",
        "enc_col": "monto_enc",
    },
]


def _ensure_columns(cur) -> None:
    """Asegura que existan las columnas cifradas en tablas objetivo."""
    cur.execute("ALTER TABLE pagos ADD COLUMN IF NOT EXISTS monto_real_enc TEXT;")
    cur.execute(
        "ALTER TABLE gastos_mensuales ADD COLUMN IF NOT EXISTS monto_presupuestado_enc TEXT;"
    )
    cur.execute("ALTER TABLE ingresos_mensuales ADD COLUMN IF NOT EXISTS monto_enc TEXT;")


def _migrate_table(cur, table: str, id_col: str, plain_col: str, enc_col: str, keep_plain: bool, dry_run: bool) -> tuple[int, int]:
    """Devuelve (cifrados, saltados)."""
    query = (
        f"SELECT {id_col} AS row_id, {plain_col} AS plain_value, {enc_col} AS enc_value "
        f"FROM {table}"
    )
    cur.execute(query)
    rows = cur.fetchall()

    updates: list[tuple[Any, ...]] = []
    skipped = 0

    for row in rows:
        row_id = row["row_id"]
        plain_value = row["plain_value"]
        enc_value = row["enc_value"]

        if enc_value:
            skipped += 1
            continue

        if plain_value is None:
            skipped += 1
            continue

        encrypted = encrypt_amount(float(plain_value))
        if encrypted is None:
            skipped += 1
            continue

        if keep_plain:
            updates.append((encrypted, row_id))
        else:
            updates.append((encrypted, row_id))

    if dry_run or not updates:
        return len(updates), skipped

    if keep_plain:
        update_sql = f"UPDATE {table} SET {enc_col} = %s WHERE {id_col} = %s;"
    else:
        update_sql = (
            f"UPDATE {table} SET {enc_col} = %s, {plain_col} = NULL "
            f"WHERE {id_col} = %s;"
        )

    psycopg2.extras.execute_batch(cur, update_sql, updates, page_size=500)
    return len(updates), skipped


def run_migration(keep_plain: bool, dry_run: bool) -> None:
    conn = get_connection()
    conn.autocommit = False

    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            _ensure_columns(cur)

            total_encrypted = 0
            total_skipped = 0

            print("\n=== Migracion de cifrado de montos ===")
            print(f"dry_run={dry_run} | keep_plain={keep_plain}\n")

            for cfg in TABLE_CONFIG:
                encrypted, skipped = _migrate_table(
                    cur=cur,
                    table=cfg["table"],
                    id_col=cfg["id_col"],
                    plain_col=cfg["plain_col"],
                    enc_col=cfg["enc_col"],
                    keep_plain=keep_plain,
                    dry_run=dry_run,
                )
                total_encrypted += encrypted
                total_skipped += skipped
                print(
                    f"[{cfg['table']}] cifrados={encrypted} | saltados={skipped}"
                )

            if dry_run:
                conn.rollback()
                print("\nDry-run completado. No se aplicaron cambios.")
            else:
                conn.commit()
                print("\nMigracion completada y confirmada (COMMIT).")

            print(
                f"Resumen total: cifrados={total_encrypted} | saltados={total_skipped}\n"
            )

    except Exception as exc:
        conn.rollback()
        print(f"\nERROR: {exc}")
        print("Se hizo ROLLBACK. No se guardaron cambios.")
        raise
    finally:
        conn.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Cifra montos historicos en tablas financieras.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Simula migracion sin aplicar cambios.",
    )
    parser.add_argument(
        "--keep-plain",
        action="store_true",
        help="Conserva columnas numericas en claro (no recomendado para privacidad).",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run_migration(keep_plain=args.keep_plain, dry_run=args.dry_run)
