# Proyecto PySpark

Implementación en Python/PySpark del pipeline medallón. La lógica vive en un
paquete reutilizable (`src/valueprops_etl/`) y los notebooks son *thin wrappers*.

## Estructura

```
pyspark_project/
├── conf/pipeline.yaml          # parámetros por defecto (override por widgets)
├── src/valueprops_etl/
│   ├── config.py               # PipelineConfig tipada (catálogo, ventanas, auditoría)
│   ├── common.py               # esquemas explícitos, write_delta, optimize/vacuum
│   ├── bronze.py               # ingesta cruda + linaje
│   ├── silver.py               # limpieza + EXPECTATIONS + dedup + particionado
│   ├── gold.py                 # compute_features (PURA) + gate de calidad + run
│   ├── quality.py              # motor de Expectations (warn/drop/fail + auditoría)
│   └── dq_rules.py             # catálogo de reglas por capa
├── notebooks/
│   ├── 00_setup.py             # widgets + bootstrap de sys.path
│   ├── 01_bronze.py · 02_silver.py · 03_gold.py
│   ├── 04_ml_feature_consumption.py  # Feature Store (UC) + MLflow
│   └── 05_data_quality.py      # reporte/gate de Expectations
├── dlt/dlt_pipeline.py         # variante NATIVA DLT/Lakeflow (@dlt.expect_*)
└── tests/
    ├── conftest.py             # SparkSession local
    ├── test_gold_features.py   # tests de compute_features
    └── test_quality.py         # tests del motor de Expectations
```

## Diseño clave

* **`compute_features` es una función pura** (recibe DataFrames, devuelve un
  DataFrame). Está separada de la E/S (`build_features` lee las tablas) para que
  sea testeable sin metastore. Los 4 tests cubren: ventana `[day-21, day-1]`,
  label `clicked`, features derivadas y filtro de "última semana".
* **Optimizaciones**: un único range-join sobre eventos unificados, `broadcast`
  del lookup de clicks, Delta con autoOptimize, particionado + ZORDER, esquemas
  explícitos, cero UDFs de Python (Photon-friendly).

## Correr en local (Mac)

```bash
pip install -r ../requirements.txt
pytest tests/ -q        # 4 passed
ruff check src/         # lint
```

## Correr en Databricks

1. Importa el repo (Repos) o despliega con el Asset Bundle (`../databricks.yml`).
2. Sube los archivos a `/Volumes/valueprops/raw/landing/`.
3. Ejecuta los notebooks `01 → 02 → 03` (o el job `valueprops_pyspark_etl`).
4. Opcional: `04_ml_feature_consumption.py` para el flujo de ML.

Parámetros vía widgets: `catalog`, `landing`. Por defecto `valueprops` y
`/Volumes/valueprops/raw/landing`.
