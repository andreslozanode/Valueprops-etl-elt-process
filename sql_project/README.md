# Proyecto SQL

Implementación 100 % declarativa en **Databricks SQL** del mismo pipeline
medallón. Cada archivo es una etapa idempotente, ejecutable desde un SQL Warehouse,
un notebook SQL o como `sql_task` de un job.

## Orden de ejecución

| # | Archivo | Qué hace |
|---|---------|----------|
| 00 | `00_setup.sql` | Crea catálogo, esquemas y el Volume de aterrizaje |
| 01 | `01_bronze.sql` | Ingesta cruda con `read_files()` + linaje |
| 02 | `02_silver.sql` | Aplana, tipa, deduplica, particiona, clusteriza, `ANALYZE` |
| 03 | `03_gold_features.sql` | Feature table con range-join (`/*+ RANGE_JOIN */`) |
| 04 | `04_optimize_maintenance.sql` | `OPTIMIZE` + `ANALYZE` + PK + `VACUUM` |

## Optimizaciones específicas de SQL

* **`/*+ RANGE_JOIN(e, 30) */`** — convierte el join de intervalo
  (`ev_day BETWEEN day-21 AND day-1`) en un range-join con bins de 30 días,
  evitando el cartesiano filtrado. Es la optimización individual de mayor impacto.
* **`schemaHints` en `read_files`** — tipado sin job de inferencia.
* **`PARTITIONED BY (day)` + `CLUSTER BY (user_id, value_prop)`** — pruning + skipping.
* **`ANALYZE TABLE ... COMPUTE STATISTICS`** — alimenta el optimizador por costos.
* **Constraints** (`NOT NULL`, `CHECK`, `PRIMARY KEY`) — calidad y compatibilidad
  con Feature Engineering de Unity Catalog.
* **Photon**: ejecuta en un SQL Warehouse Pro/Serverless para vectorización.

## Cómo correrlo

```sql
-- En un SQL editor / notebook, ejecuta en orden 00 → 01 → 02 → 03 → 04.
-- O como job (ver ../databricks.yml -> valueprops_sql_etl), pasando warehouse_id.
```

> Antes de `01`, sube los 3 archivos a `/Volumes/valueprops/bronze/landing/`.

El resultado es idéntico al del proyecto PySpark: `valueprops.gold.value_prop_features`
(113.336 filas).
