-- ===========================================================================
-- 04 · Mantenimiento y preparación para ML
-- Compactación, estadísticas, retención y clave primaria para Feature Store.
-- Programar como tarea final del job (o en un schedule nocturno).
-- ===========================================================================

-- Compactar + co-localizar por las columnas de lookup del modelo.
-- (Si la tabla usa Liquid Clustering, basta `OPTIMIZE ... FULL`.)
OPTIMIZE valueprops.gold.value_prop_features ZORDER BY (user_id, value_prop);
OPTIMIZE valueprops.silver.prints           ZORDER BY (user_id, value_prop);
OPTIMIZE valueprops.silver.taps             ZORDER BY (user_id, value_prop);
OPTIMIZE valueprops.silver.pays             ZORDER BY (user_id, value_prop);

-- Estadísticas para el CBO sobre el Gold.
ANALYZE TABLE valueprops.gold.value_prop_features COMPUTE STATISTICS FOR ALL COLUMNS;

-- Clave primaria -> requisito para registrar la tabla en Feature Engineering (UC)
-- y habilitar lookups deterministas en entrenamiento/serving.
ALTER TABLE valueprops.gold.value_prop_features
  ADD CONSTRAINT vp_pk PRIMARY KEY (day, user_id, value_prop, position);

-- Retención: conserva 7 días de historial de versiones (time travel) y purga el resto.
VACUUM valueprops.gold.value_prop_features RETAIN 168 HOURS;
VACUUM valueprops.silver.prints RETAIN 168 HOURS;
VACUUM valueprops.silver.taps   RETAIN 168 HOURS;
VACUUM valueprops.silver.pays   RETAIN 168 HOURS;
