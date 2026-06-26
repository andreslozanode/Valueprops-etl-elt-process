# Data Quality — Expectations

El proyecto valida la calidad de datos con **Expectations** en cada capa, con la
misma semántica en las tres implementaciones disponibles.

## Las tres acciones

| Acción | Qué hace con la fila que viola | Cuándo usarla |
|--------|--------------------------------|---------------|
| `warn` | La deja pasar y registra la violación | Anomalías a vigilar (p. ej. `value_prop` desconocido) |
| `drop` | La descarta (cuarentena opcional) | Filas inservibles pero no críticas (p. ej. `total < 0`) |
| `fail` | **Aborta** el pipeline | Invariantes que romperían el contrato (p. ej. `user_id IS NULL`) |

## Reglas activas

**Silver (prints/taps)**: `user_id_not_null` (fail), `value_prop_not_null` (drop),
`value_prop_known` (warn), `day_not_null` (fail), `day_not_future` (warn),
`position_in_range` (warn).
**Silver (pays)**: `user_id_not_null` (fail), `day_not_null` (fail),
`total_non_negative` (drop), `value_prop_known` (warn).
**Gold**: `clicked_binary` (fail), `counts_non_negative` (fail),
`amount_non_negative` (fail), `pk_complete` (fail), `amount_matches_pays` (warn),
`ctr_bounded` (warn).

Las reglas viven en un solo lugar editable:
`pyspark_project/src/valueprops_etl/dq_rules.py`.

## Tres formas de aplicarlas (elige según tu setup)

### 1. Motor portable (jobs clásicos) — *por defecto en este repo*
`valueprops_etl.quality.apply_expectations(...)` evalúa todas las reglas en una
sola pasada, escribe el resultado en `valueprops.monitoring.dq_results` y aplica
`drop`/`fail`. Ya está integrado en `silver.run()` y `gold.run()`. El notebook
`05_data_quality.py` muestra el reporte y actúa como *gate* de monitoreo.

### 2. Nativo DLT / Lakeflow (`pyspark_project/dlt/dlt_pipeline.py`)
Usa `@dlt.expect`, `@dlt.expect_or_drop`, `@dlt.expect_or_fail` y
`@dlt.expect_all_or_*`. Databricks captura las métricas de calidad en el *event
log* del pipeline automáticamente. Es el camino más declarativo y gestionado.

### 3. SQL (`sql_project/sql/05_quality_checks.sql`)
Combina constraints de tabla (`NOT NULL`/`CHECK`), una tabla de auditoría
`dq_results`, un `raise_error()` que actúa como ASSERT, una tabla de cuarentena y
la vista `dq_latest` para dashboards.

## Observabilidad

Todas las corridas alimentan `valueprops.monitoring.dq_results`. La vista
`valueprops.monitoring.dq_latest` da la última foto por tabla/expectation, lista para
conectarse a un dashboard SQL o a una alerta de Databricks.

## Alertas SQL (Databricks SQL Alerts v2)

El bundle define dos alertas sobre `dq_latest` (en `databricks.yml`, recurso
`alerts`). Corren diariamente a las 7:00 AM (hora de Bogotá) y notifican por correo:

| Alerta | Query | Dispara si | Acción |
|--------|-------|-----------|--------|
| `dq_critical_failures` | `count(*) WHERE action='fail' AND passed=false` | `> 0` | Hay una expectation crítica en rojo |
| `dq_failure_rate` | `max(failure_pct)` | `> 1%` | Degradación de la fuente a vigilar |

Configura el destinatario y el warehouse al desplegar:

```bash
databricks bundle deploy -t prod \
  --var="sql_warehouse_id=<id>" \
  --var="alert_email=tu-email@example.com"
```

> Nota: Databricks SQL Alerts v2 está en *Public Preview*; requiere Databricks
> CLI ≥ 0.279. Si tu CLI es anterior, crea las alertas desde la UI usando las
> mismas queries, o usa el recurso Terraform `databricks_alert_v2`.

## Resultado validado sobre los datos reales

Las 22 expectations pasan con 0 violaciones (los datos de noviembre 2020 están
limpios) y la feature table se mantiene en 113.336 filas tras el gate de calidad.
