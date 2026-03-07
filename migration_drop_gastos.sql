-- ============================================================
-- migration_drop_gastos.sql
-- Corre este script UNA SOLA VEZ en Neon antes de desplegar
-- el código nuevo.
--
-- Qué hace:
--   1. Agrega columnas nombre, categoria, periodicidad,
--      fecha_pago, fecha_inicio, fecha_termino a gastos_mensuales
--      (y a pagos, para mantener nombre/categoria en historial).
--   2. Copia los datos desde gastos → gastos_mensuales.
--   3. Genera un gasto_id autoincremental propio ≪sin FK a gastos≫.
--   4. Elimina la tabla pagos (los pagos ya no referencian gastos).
--   5. Elimina la tabla gastos.
-- ============================================================

BEGIN;

-- ── 1. Agregar columnas a gastos_mensuales ──────────────────
ALTER TABLE gastos_mensuales
    ADD COLUMN IF NOT EXISTS nombre       TEXT,
    ADD COLUMN IF NOT EXISTS categoria    TEXT,
    ADD COLUMN IF NOT EXISTS periodicidad TEXT,
    ADD COLUMN IF NOT EXISTS fecha_pago   INTEGER,
    ADD COLUMN IF NOT EXISTS fecha_inicio DATE,
    ADD COLUMN IF NOT EXISTS fecha_termino DATE;

-- ── 2. Copiar nombre/categoria/etc desde gastos ─────────────
UPDATE gastos_mensuales gm
SET
    nombre        = g.nombre,
    categoria     = g.categoria,
    periodicidad  = g.periodicidad,
    fecha_pago    = CASE WHEN g.periodicidad = 'Mensual' THEN g.fecha_pago ELSE NULL END,
    fecha_inicio  = g.fecha_inicio,
    fecha_termino = g.fecha_termino
FROM gastos g
WHERE gm.gasto_id = g.gasto_id;

-- ── 3. Agregar columnas nombre/categoria a pagos ────────────
--    (para conservar el historial de pagos sin la FK a gastos)
ALTER TABLE pagos
    ADD COLUMN IF NOT EXISTS nombre    TEXT,
    ADD COLUMN IF NOT EXISTS categoria TEXT;

UPDATE pagos p
SET
    nombre    = g.nombre,
    categoria = g.categoria
FROM gastos g
WHERE p.gasto_id = g.gasto_id;

-- ── 4. Quitar FK de pagos → gastos ─────────────────────────
--    Primero averigua el nombre del constraint con:
--    SELECT conname FROM pg_constraint WHERE conrelid = 'pagos'::regclass;
--    Luego reemplaza 'pagos_gasto_id_fkey' con el nombre real si difiere.
ALTER TABLE pagos DROP CONSTRAINT IF EXISTS pagos_gasto_id_fkey;

-- ── 5. Quitar FK de gastos_mensuales → gastos ──────────────
ALTER TABLE gastos_mensuales DROP CONSTRAINT IF EXISTS gastos_mensuales_gasto_id_fkey;

-- ── 6. Eliminar tabla gastos ────────────────────────────────
DROP TABLE IF EXISTS gastos CASCADE;

-- ── 7. Crear secuencia propia para gastos_mensuales.gasto_id ──
--    (ahora gasto_id es solo un agrupador lógico de filas del mismo gasto)
--    Si quieres que el campo sea auto-manejado desde la app no hace falta
--    cambiar nada más; la app generará los IDs con _next_id().

COMMIT;
