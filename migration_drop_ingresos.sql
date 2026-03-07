-- ============================================================
-- migration_drop_ingresos.sql
-- Migra datos de la tabla `ingresos` a `ingresos_mensuales`
-- y elimina la tabla `ingresos`.
--
-- INSTRUCCIONES:
--   1. Ejecuta este script UNA SOLA VEZ en la consola SQL de Neon.
--   2. Verifica con SELECT COUNT(*) FROM ingresos_mensuales; que los datos quedaron.
--   3. Luego puedes ejecutar DROP TABLE ingresos CASCADE; con seguridad.
-- ============================================================

BEGIN;

-- 1. Añadir columnas nuevas a ingresos_mensuales
ALTER TABLE ingresos_mensuales
    ADD COLUMN IF NOT EXISTS nombre        TEXT,
    ADD COLUMN IF NOT EXISTS periodicidad  TEXT,
    ADD COLUMN IF NOT EXISTS fecha_pago    INTEGER,
    ADD COLUMN IF NOT EXISTS fecha_inicio  DATE,
    ADD COLUMN IF NOT EXISTS fecha_termino DATE,
    ADD COLUMN IF NOT EXISTS user_id       INTEGER REFERENCES users(id);

-- 2. Copiar metadata de ingresos a las filas existentes en ingresos_mensuales
UPDATE ingresos_mensuales im
SET
    nombre        = i.nombre,
    periodicidad  = i.periodicidad,
    fecha_pago    = i.fecha_pago,
    fecha_inicio  = i.fecha_inicio,
    fecha_termino = i.fecha_termino,
    user_id       = i.user_id
FROM ingresos i
WHERE im.ingreso_id = i.ingreso_id;

-- 3. Crear filas faltantes en ingresos_mensuales para meses que no se generaron antes
-- (basado en ingresos de periodicidad Mensual: genera 12 meses para el año actual)
-- Si tienes datos en ingresos que aún no tienen filas en ingresos_mensuales, ajusta aquí.

-- 4. Eliminar la clave foránea antes de hacer DROP
ALTER TABLE ingresos_mensuales
    DROP CONSTRAINT IF EXISTS ingresos_mensuales_ingreso_id_fkey;

-- 5. Eliminar la tabla ingresos
DROP TABLE IF EXISTS ingresos CASCADE;

COMMIT;

-- Verificación final
SELECT COUNT(*) AS total_ingresos_mensuales FROM ingresos_mensuales;
