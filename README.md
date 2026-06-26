# Value Props · ETL/ELT en Databricks

Pipeline de datos que, a partir de tres fuentes de eventos (impresiones, clicks y
pagos de *value props*), construye una **tabla de features ML-ready** con la
arquitectura medallón **Bronze → Silver → Gold** sobre **Delta Lake** y **Unity Catalog**.

El repositorio contiene **dos implementaciones independientes y equivalentes** del
mismo pipeline:

| Proyecto | Carpeta | Stack | Cuándo usarlo |
|----------|---------|-------|---------------|
| **PySpark** | [`pyspark_project/`](pyspark_project/) | Python · PySpark · pytest · DAB | Lógica compleja, reutilizable y testeable unitariamente |
| **SQL** | [`sql_project/`](sql_project/) | Databricks SQL · Delta | Transformaciones declarativas, analistas, SQL warehouses |

Ambas producen **exactamente la misma salida**: `gold.value_prop_features`
(113.336 filas, validado de punta a punta contra una implementación de referencia).

## El problema

Para cada **print de la última semana**, calcular:

1. `clicked` — si el usuario hizo tap sobre ese print (**label** del modelo).
2. `views_3w` — # veces que vio ese value_prop en las **3 semanas previas**.
3. `taps_3w` — # veces que lo tapeó en las 3 semanas previas.
4. `pays_3w` — # pagos de ese value_prop en las 3 semanas previas.
5. `amount_3w` — monto acumulado pagado de ese value_prop en las 3 semanas previas.

Más dos features derivadas (`ctr_3w`, `avg_ticket_3w`) listas para entrenamiento.
Detalle de reglas en [`docs/assumptions.md`](docs/assumptions.md).

## Arquitectura

```
prints.json ┐                                          ┌─ gold.value_prop_features ─┐
taps.json   ├─► Bronze (raw+linaje) ─► Silver (limpio) ─┤   (features + label)        ├─► Feature Store (UC)
pays.csv    ┘                          part. + cluster  └─────────────────────────────┘     + MLflow
```

Ver el diagrama completo y la justificación del *single range-join* en
[`docs/architecture.md`](docs/architecture.md).

## Optimizaciones implementadas

| Técnica | PySpark | SQL | Beneficio |
|---------|:------:|:---:|-----------|
| Formato Delta + `optimizeWrite`/`autoCompact` | ✔ | ✔ | Evita el *small files problem* |
| Particionado por `day` | ✔ | ✔ | *Partition pruning* en la ventana de 3 semanas |
| ZORDER / Liquid Clustering `(user_id, value_prop)` | ✔ | ✔ | *Data-skipping* en join y filtros |
| **Único range-join** sobre eventos unificados | ✔ | ✔ | 3 shuffles → 1 |
| `RANGE_JOIN` hint | — | ✔ | Evita el cartesiano filtrado del join de intervalo |
| `broadcast` del lookup de clicks | ✔ | (auto) | Sin shuffle del lado pequeño |
| Esquemas explícitos (sin inferencia) | ✔ | ✔ (schemaHints) | Un job menos sobre 500k+ líneas |
| Sin UDFs de Python | ✔ | ✔ | Compatible con Photon / vectorizado |
| `ANALYZE TABLE` (estadísticas CBO) | — | ✔ | Mejores planes |
| `OPTIMIZE` + `VACUUM` | ✔ | ✔ | Compactación y control de costo |

## Calidad de datos (Expectations)

El pipeline valida la calidad en cada capa con **Expectations** (semántica
`warn` / `drop` / `fail`), disponibles en tres formas: un **motor portable**
integrado en los jobs clásicos (escribe auditoría en `monitoring.dq_results`),
una variante **nativa DLT/Lakeflow** (`@dlt.expect_*`) y una versión **SQL** con
`raise_error()` como ASSERT + cuarentena. Las 22 reglas pasan con 0 violaciones
sobre los datos reales. Detalle en [`docs/data_quality.md`](docs/data_quality.md).

## CI/CD y calidad local

* **GitHub Actions** ([`.github/workflows/ci.yml`](.github/workflows/ci.yml)): en cada
  push corre `ruff` (lint) + `mypy` (tipos) + `pytest` (10 tests) +
  `databricks bundle validate`.
* **Pre-commit** ([`.pre-commit-config.yaml`](.pre-commit-config.yaml)): en cada
  commit corre ruff + formato + checks de archivos (incluido un guard que impide
  subir los datasets crudos por error); en cada `push` corre `pytest`. Instalación:
  `pip install pre-commit && pre-commit install`.

## Alertas de calidad

El bundle define dos **Databricks SQL Alerts (v2)** sobre `monitoring.dq_latest`
que notifican por correo si una expectation crítica falla o si la tasa de fallos
supera 1%. Ver [`docs/data_quality.md`](docs/data_quality.md).

## Listo para ML/AI

* Salida con **clave primaria** `(day, user_id, value_prop, position)` y **label** `clicked`.
* Features numéricas sin nulos → consumibles directamente por modelos.
* `CHECK`/`NOT NULL` constraints → datos confiables para entrenar.
* Change Data Feed habilitado en el Gold → *serving* incremental.
* Notebook [`04_ml_feature_consumption.py`](pyspark_project/notebooks/04_ml_feature_consumption.py)
  registra la tabla en **Feature Engineering (Unity Catalog)**, crea un *training set*
  por lookup, entrena un baseline con **MLflow** y deja el modelo listo para
  *batch/online serving* (sin *training/serving skew*).

## Quickstart

```bash
# 1. Local: tests del proyecto PySpark (Mac)
cd pyspark_project
pip install -r ../requirements.txt
pytest tests/ -q

# 2. Databricks: subir los 3 archivos al Volume de aterrizaje
#    /Volumes/valueprops/bronze/landing/{prints.json, taps.json, pays.csv}

# 3. Desplegar ambos jobs con Databricks Asset Bundles
databricks bundle validate
databricks bundle deploy -t dev
databricks bundle run valueprops_pyspark_etl -t dev      # o valueprops_sql_etl
```

## Estructura del repositorio

```
valueprops-etl/
├── README.md                 ← este archivo (manifest)
├── databricks.yml            ← Bundle: 2 jobs + 1 pipeline DLT + 2 alertas SQL
├── requirements.txt          ← deps de desarrollo/test local
├── .gitignore                ← excluye datos crudos (tamaño + PII)
├── .pre-commit-config.yaml   ← hooks: ruff/format/checks (commit) + pytest (push)
├── .github/workflows/ci.yml  ← CI: ruff + mypy + pytest + bundle validate
├── docs/
│   ├── architecture.md
│   ├── data_dictionary.md
│   ├── assumptions.md
│   └── data_quality.md       ← expectations, observabilidad y alertas
├── pyspark_project/          ← Proyecto A (Python/PySpark)
│   ├── src/valueprops_etl/   ← config, bronze, silver, gold, common,
│   │                            quality (motor), dq_rules (catálogo)
│   ├── notebooks/            ← 00..05 (thin wrappers + ML + DQ)
│   ├── dlt/dlt_pipeline.py   ← variante nativa DLT/Lakeflow
│   └── tests/                ← pytest (features + expectations)
└── sql_project/              ← Proyecto B (Databricks SQL)
    └── sql/                  ← 00..05 (setup, bronze, silver, gold,
                                 mantenimiento, quality_checks)
```

## Datos

Los archivos crudos **no se versionan** (ver `.gitignore`). Esquemas y perfilado
completo en [`docs/data_dictionary.md`](docs/data_dictionary.md). En Databricks se
cargan a un Volume de Unity Catalog.

## Licencia

MIT — ver [`LICENSE`](LICENSE).
