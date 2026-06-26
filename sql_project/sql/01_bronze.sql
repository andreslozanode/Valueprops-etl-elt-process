-- ===========================================================================
-- 01 · Bronze — Ingesta cruda con read_files() (TVF nativa de Databricks)
-- Fiel a la fuente + metadatos de linaje. Idempotente (CREATE OR REPLACE).
-- ===========================================================================

-- PRINTS (JSON, una línea por evento)
CREATE OR REPLACE TABLE valueprops.bronze.prints_raw
TBLPROPERTIES (
  delta.autoOptimize.optimizeWrite = true,
  delta.autoOptimize.autoCompact   = true
) AS
SELECT
  day,
  event_data,                          -- struct<position, value_prop> sin aplanar
  user_id,
  current_timestamp() AS _ingested_at,
  _metadata.file_path AS _source_file
FROM read_files(
  '/Volumes/valueprops/bronze/landing/prints.json',
  format        => 'json',
  multiLine     => false,
  schemaHints   => 'user_id BIGINT'
);

-- TAPS (mismo esquema que prints)
CREATE OR REPLACE TABLE valueprops.bronze.taps_raw
TBLPROPERTIES (
  delta.autoOptimize.optimizeWrite = true,
  delta.autoOptimize.autoCompact   = true
) AS
SELECT
  day, event_data, user_id,
  current_timestamp() AS _ingested_at,
  _metadata.file_path AS _source_file
FROM read_files(
  '/Volumes/valueprops/bronze/landing/taps.json',
  format => 'json', multiLine => false, schemaHints => 'user_id BIGINT'
);

-- PAYS (CSV con header)
CREATE OR REPLACE TABLE valueprops.bronze.pays_raw
TBLPROPERTIES (
  delta.autoOptimize.optimizeWrite = true,
  delta.autoOptimize.autoCompact   = true
) AS
SELECT
  pay_date, total, user_id, value_prop,
  current_timestamp() AS _ingested_at,
  _metadata.file_path AS _source_file
FROM read_files(
  '/Volumes/valueprops/bronze/landing/pays.csv',
  format => 'csv', header => true,
  schemaHints => 'pay_date DATE, total DOUBLE, user_id BIGINT, value_prop STRING'
);
