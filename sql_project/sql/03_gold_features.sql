-- ===========================================================================
-- 03 · Gold — Feature table ML-ready (prints de la última semana)
--
-- Para cada print de la última semana calcula, en las 3 semanas previas
-- [day-21, day-1] del mismo (user_id, value_prop):
--   views_3w, taps_3w, pays_3w, amount_3w  + features derivadas + label clicked
--
-- OPTIMIZACIÓN CLAVE — el hint RANGE_JOIN:
--   El join de intervalo (e.ev_day BETWEEN day-21 AND day-1) sin ayuda degenera
--   en un join cartesiano filtrado. /*+ RANGE_JOIN(e, 30) */ activa el
--   range-join optimization de Databricks: bucketiza el tiempo en bins de 30
--   días y solo compara filas del mismo bin. Reduce drásticamente la
--   complejidad. El bin (30) se ajusta a la longitud de la ventana (21).
-- ===========================================================================

CREATE OR REPLACE TABLE valueprops.gold.value_prop_features (
  day            DATE    NOT NULL,
  user_id        BIGINT  NOT NULL,
  value_prop     STRING  NOT NULL,
  position       INT,
  clicked        INT,
  views_3w       BIGINT,
  taps_3w        BIGINT,
  pays_3w        BIGINT,
  amount_3w      DOUBLE,
  ctr_3w         DOUBLE,
  avg_ticket_3w  DOUBLE,
  _computed_at   TIMESTAMP,
  CONSTRAINT clicked_binary CHECK (clicked IN (0, 1))
)
PARTITIONED BY (day)
CLUSTER BY (user_id, value_prop)
TBLPROPERTIES (
  delta.autoOptimize.optimizeWrite = true,
  delta.autoOptimize.autoCompact   = true,
  delta.enableChangeDataFeed        = true   -- habilita CDC para serving incremental/ML
);

INSERT OVERWRITE valueprops.gold.value_prop_features
WITH bounds AS (SELECT max(day) AS max_day FROM valueprops.silver.prints),
last_week AS (
  SELECT p.*
  FROM valueprops.silver.prints p, bounds b
  WHERE p.day >= date_sub(b.max_day, 7 - 1)        -- últimos 7 días
),
events AS (
  SELECT user_id, value_prop, day AS ev_day, 'print' AS etype, CAST(0 AS DOUBLE) AS amount
  FROM valueprops.silver.prints
  UNION ALL
  SELECT user_id, value_prop, day, 'tap', CAST(0 AS DOUBLE) FROM valueprops.silver.taps
  UNION ALL
  SELECT user_id, value_prop, day, 'pay', total FROM valueprops.silver.pays
),
clicked AS (
  SELECT DISTINCT day, user_id, value_prop, 1 AS clicked FROM valueprops.silver.taps
),
agg AS (
  SELECT /*+ RANGE_JOIN(e, 30) */
    b.day, b.user_id, b.value_prop, b.position,
    SUM(CASE WHEN e.etype = 'print' THEN 1 ELSE 0 END)              AS views_3w,
    SUM(CASE WHEN e.etype = 'tap'   THEN 1 ELSE 0 END)              AS taps_3w,
    SUM(CASE WHEN e.etype = 'pay'   THEN 1 ELSE 0 END)              AS pays_3w,
    COALESCE(SUM(CASE WHEN e.etype = 'pay' THEN e.amount END), 0.0) AS amount_3w
  FROM last_week b
  LEFT JOIN events e
    ON  e.user_id    = b.user_id
    AND e.value_prop = b.value_prop
    AND e.ev_day BETWEEN date_sub(b.day, 21) AND date_sub(b.day, 1)
  GROUP BY b.day, b.user_id, b.value_prop, b.position
)
SELECT
  a.day, a.user_id, a.value_prop, a.position,
  COALESCE(c.clicked, 0)                                                AS clicked,
  a.views_3w, a.taps_3w, a.pays_3w, a.amount_3w,
  CASE WHEN a.views_3w > 0 THEN a.taps_3w   / a.views_3w ELSE 0.0 END   AS ctr_3w,
  CASE WHEN a.pays_3w  > 0 THEN a.amount_3w / a.pays_3w  ELSE 0.0 END   AS avg_ticket_3w,
  current_timestamp()                                                   AS _computed_at
FROM agg a
LEFT JOIN clicked c
  ON c.day = a.day AND c.user_id = a.user_id AND c.value_prop = a.value_prop;
