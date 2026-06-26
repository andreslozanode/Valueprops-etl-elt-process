-- ===========================================================================
-- 02 · Silver — Limpieza, aplanado, tipado, dedup, particionado
-- Aplana event_data, castea a DATE, deduplica por clave natural.
-- Optimización: PARTITIONED BY (day) + CLUSTER BY (Liquid Clustering) sobre
-- las columnas de join/filtro del Gold (user_id, value_prop).
-- ===========================================================================

-- ---------------- PRINTS ----------------
CREATE OR REPLACE TABLE valueprops.silver.prints (
  day        DATE,
  user_id    BIGINT,
  value_prop STRING,
  position   INT
)
PARTITIONED BY (day)
CLUSTER BY (user_id, value_prop)
TBLPROPERTIES (
  delta.autoOptimize.optimizeWrite = true,
  delta.autoOptimize.autoCompact   = true
);

INSERT OVERWRITE valueprops.silver.prints
SELECT DISTINCT
  CAST(day AS DATE)        AS day,
  user_id,
  event_data.value_prop    AS value_prop,
  event_data.position      AS position
FROM valueprops.bronze.prints_raw
WHERE event_data.value_prop IS NOT NULL AND user_id IS NOT NULL;

-- ---------------- TAPS ----------------
CREATE OR REPLACE TABLE valueprops.silver.taps (
  day        DATE,
  user_id    BIGINT,
  value_prop STRING,
  position   INT
)
PARTITIONED BY (day)
CLUSTER BY (user_id, value_prop)
TBLPROPERTIES (
  delta.autoOptimize.optimizeWrite = true,
  delta.autoOptimize.autoCompact   = true
);

INSERT OVERWRITE valueprops.silver.taps
SELECT DISTINCT
  CAST(day AS DATE)        AS day,
  user_id,
  event_data.value_prop    AS value_prop,
  event_data.position      AS position
FROM valueprops.bronze.taps_raw
WHERE event_data.value_prop IS NOT NULL AND user_id IS NOT NULL;

-- ---------------- PAYS ----------------
-- NO se deduplica: dos pagos idénticos el mismo día son eventos legítimos.
CREATE OR REPLACE TABLE valueprops.silver.pays (
  day        DATE,
  user_id    BIGINT,
  value_prop STRING,
  total      DOUBLE
)
PARTITIONED BY (day)
CLUSTER BY (user_id, value_prop)
TBLPROPERTIES (
  delta.autoOptimize.optimizeWrite = true,
  delta.autoOptimize.autoCompact   = true
);

INSERT OVERWRITE valueprops.silver.pays
SELECT
  CAST(pay_date AS DATE) AS day,
  user_id,
  value_prop,
  CAST(total AS DOUBLE)  AS total
FROM valueprops.bronze.pays_raw
WHERE value_prop IS NOT NULL AND user_id IS NOT NULL;

-- Estadísticas para el optimizador basado en costos (CBO)
ANALYZE TABLE valueprops.silver.prints COMPUTE STATISTICS FOR ALL COLUMNS;
ANALYZE TABLE valueprops.silver.taps   COMPUTE STATISTICS FOR ALL COLUMNS;
ANALYZE TABLE valueprops.silver.pays   COMPUTE STATISTICS FOR ALL COLUMNS;
