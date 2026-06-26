-- ===========================================================================
-- 05 · Data Quality — Expectations en SQL
--
-- Tres mecanismos nativos de Databricks SQL:
--   (A) Constraints declarativas en la tabla (NOT NULL / CHECK)  -> ya en 02/03.
--   (B) ASSERT vía consulta que falla el task si hay violaciones (acción 'fail').
--   (C) Cuarentena: filas inválidas se desvían a una tabla *_quarantine.
--   (D) Tabla/vista de auditoría con el resultado de cada expectation.
--
-- Ejecutar DESPUÉS de 02_silver.sql y 03_gold_features.sql.
-- ===========================================================================

CREATE SCHEMA IF NOT EXISTS valueprops.monitoring;

-- ---------------------------------------------------------------------------
-- (D) Resultados de expectations -> tabla de auditoría (append por corrida)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS valueprops.monitoring.dq_results (
  run_id        STRING,
  layer         STRING,
  table_name    STRING,
  expectation   STRING,
  action        STRING,
  total_rows    BIGINT,
  failed_rows   BIGINT,
  failure_rate  DOUBLE,
  passed        BOOLEAN,
  checked_at    TIMESTAMP
);

INSERT INTO valueprops.monitoring.dq_results
WITH rid AS (SELECT uuid() AS run_id, current_timestamp() AS ts)
-- Silver prints
SELECT rid.run_id, 'silver', 'prints', 'user_id_not_null', 'fail',
       count(*), count_if(user_id IS NULL),
       count_if(user_id IS NULL)/count(*), count_if(user_id IS NULL)=0, rid.ts
FROM valueprops.silver.prints, rid GROUP BY rid.run_id, rid.ts
UNION ALL
SELECT rid.run_id, 'silver', 'prints', 'value_prop_known', 'warn',
       count(*), count_if(value_prop NOT IN ('cellphone_recharge','credits_consumer','link_cobro','point','prepaid','send_money','transport')),
       count_if(value_prop NOT IN ('cellphone_recharge','credits_consumer','link_cobro','point','prepaid','send_money','transport'))/count(*),
       count_if(value_prop NOT IN ('cellphone_recharge','credits_consumer','link_cobro','point','prepaid','send_money','transport'))=0, rid.ts
FROM valueprops.silver.prints, rid GROUP BY rid.run_id, rid.ts
UNION ALL
-- Silver pays
SELECT rid.run_id, 'silver', 'pays', 'total_non_negative', 'drop',
       count(*), count_if(total < 0), count_if(total < 0)/count(*), count_if(total < 0)=0, rid.ts
FROM valueprops.silver.pays, rid GROUP BY rid.run_id, rid.ts
UNION ALL
-- Gold feature table
SELECT rid.run_id, 'gold', 'value_prop_features', 'clicked_binary', 'fail',
       count(*), count_if(clicked NOT IN (0,1)), count_if(clicked NOT IN (0,1))/count(*), count_if(clicked NOT IN (0,1))=0, rid.ts
FROM valueprops.gold.value_prop_features, rid GROUP BY rid.run_id, rid.ts
UNION ALL
SELECT rid.run_id, 'gold', 'value_prop_features', 'counts_non_negative', 'fail',
       count(*), count_if(views_3w<0 OR taps_3w<0 OR pays_3w<0),
       count_if(views_3w<0 OR taps_3w<0 OR pays_3w<0)/count(*),
       count_if(views_3w<0 OR taps_3w<0 OR pays_3w<0)=0, rid.ts
FROM valueprops.gold.value_prop_features, rid GROUP BY rid.run_id, rid.ts
UNION ALL
SELECT rid.run_id, 'gold', 'value_prop_features', 'pk_unique', 'fail',
       count(*), count(*) - count(DISTINCT day, user_id, value_prop, position),
       (count(*) - count(DISTINCT day, user_id, value_prop, position))/count(*),
       count(*) = count(DISTINCT day, user_id, value_prop, position), rid.ts
FROM valueprops.gold.value_prop_features, rid GROUP BY rid.run_id, rid.ts;

-- ---------------------------------------------------------------------------
-- (B) ASSERT: aborta el task si alguna expectation 'fail' de la última corrida
--     no pasó. (raise_error corta la ejecución del job.)
-- ---------------------------------------------------------------------------
SELECT
  CASE WHEN count_if(action = 'fail' AND NOT passed) > 0
       THEN raise_error(
              concat('Data Quality FAIL: ',
                     cast(count_if(action = 'fail' AND NOT passed) AS STRING),
                     ' expectations críticas violadas'))
       ELSE 'DQ OK'
  END AS dq_gate
FROM valueprops.monitoring.dq_results
WHERE run_id = (SELECT max(run_id) FROM valueprops.monitoring.dq_results
                ORDER BY checked_at DESC LIMIT 1);

-- ---------------------------------------------------------------------------
-- (C) Cuarentena (ejemplo para pays): desvía filas inválidas en vez de perderlas.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS valueprops.monitoring.pays_quarantine AS
SELECT *, current_timestamp() AS _quarantined_at
FROM valueprops.silver.pays WHERE 1 = 0;   -- estructura vacía

-- (En un pipeline real, este INSERT correría sobre la zona Bronze->Silver,
--  antes del filtro de calidad, capturando lo que la regla 'drop' descarta.)

-- ---------------------------------------------------------------------------
-- Vista de monitoreo: última foto de calidad por tabla/expectation.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW valueprops.monitoring.dq_latest AS
SELECT layer, table_name, expectation, action, total_rows, failed_rows,
       round(failure_rate * 100, 2) AS failure_pct, passed, checked_at
FROM valueprops.monitoring.dq_results
QUALIFY row_number() OVER (
          PARTITION BY layer, table_name, expectation
          ORDER BY checked_at DESC) = 1;
