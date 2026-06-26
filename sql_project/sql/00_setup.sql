-- ===========================================================================
-- 00 · Setup — Catálogo y esquemas (Unity Catalog)
-- Ejecutar una vez. Parametriza el catálogo con :catalog si usas un SQL task.
-- ===========================================================================
CREATE CATALOG IF NOT EXISTS valueprops;

CREATE SCHEMA IF NOT EXISTS valueprops.bronze;
CREATE SCHEMA IF NOT EXISTS valueprops.silver;
CREATE SCHEMA IF NOT EXISTS valueprops.gold;

-- Volumen gestionado para aterrizar los archivos crudos (prints/taps/pays).
-- Sube allí los 3 archivos antes de correr 01_bronze.sql.
CREATE VOLUME IF NOT EXISTS valueprops.bronze.landing;
-- Ruta resultante: /Volumes/valueprops/bronze/landing/{prints.json, taps.json, pays.csv}
