# Arquitectura

## Arquitectura medallón (Bronze → Silver → Gold)

```
   Archivos crudos                BRONZE                    SILVER                       GOLD
 (Volume UC / landing)        (fiel a la fuente)    (limpio, tipado, particionado)   (features ML-ready)
┌──────────────────┐        ┌────────────────┐      ┌──────────────────────┐       ┌────────────────────────┐
│ prints.json      │──────► │ bronze.prints_ │────► │ silver.prints        │──┐    │ gold.value_prop_       │
│ taps.json        │──────► │   raw / taps_  │────► │ silver.taps          │  ├──► │   features             │
│ pays.csv         │──────► │   raw / pays_  │────► │ silver.pays          │──┘    │ (última semana +       │
└──────────────────┘        │   raw          │      │ part. by day,        │       │  agregados de 3 sem.   │
   read_files()/            │ + linaje       │      │ ZORDER/CLUSTER BY    │       │  + label clicked)      │
   spark.read.schema        └────────────────┘      │ (user_id,value_prop) │       └────────────┬───────────┘
                                                     └──────────────────────┘                    │
                                                                                                  ▼
                                                                              Feature Engineering (UC) + MLflow
                                                                              training set · model · batch/online serving
```

## Responsabilidad de cada capa

| Capa   | Qué hace                                                            | Qué NO hace                          |
|--------|--------------------------------------------------------------------|--------------------------------------|
| Bronze | Ingesta fiel + metadatos (`_ingested_at`, `_source_file`)          | No aplica reglas de negocio          |
| Silver | Aplana, castea a DATE, deduplica, particiona, clusteriza           | No agrega ni cruza fuentes           |
| Gold   | Une eventos, calcula ventana de 3 semanas, deriva features y label | No re-limpia (confía en Silver)      |

## El corazón del Gold: un único *range-join*

El requisito "contar eventos en las 3 semanas previas a cada print" es un **join
de intervalo** (no-equi join). En lugar de hacer 3 joins separados (prints, taps,
pays) se hace **una unión tipada de eventos** y **un solo range-join**:

```
eventos = prints ∪ taps ∪ pays   (con columna etype y amount)
features = last_week_prints  ⋈[user_id, value_prop, ev_day ∈ [day-21, day-1]]  eventos
         → agregación condicional (SUM CASE WHEN etype = ...) en una sola pasada
```

Esto reduce **3 shuffles costosos a 1** y deriva las 4 métricas simultáneamente.

* **PySpark**: `Photon + AQE` manejan el skew; el lookup de `clicked` va por `broadcast`.
* **SQL**: el hint `/*+ RANGE_JOIN(e, 30) */` bucketiza el tiempo para evitar el
  cartesiano filtrado.

## Flujo de orquestación (Databricks Asset Bundle)

```
bronze ──► silver ──► gold ──► (maintenance | ml_consumption)
```

Definido en `databricks.yml` como dos jobs independientes
(`valueprops_pyspark_etl` y `valueprops_sql_etl`).
