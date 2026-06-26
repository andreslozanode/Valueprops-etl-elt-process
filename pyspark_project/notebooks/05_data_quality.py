# Databricks notebook source
# MAGIC %md
# MAGIC # 05 · Data Quality — Reporte de Expectations
# MAGIC Lee la tabla de auditoría que los jobs Silver/Gold escriben en cada corrida
# MAGIC y muestra el estado de calidad. Útil como tarea de monitoreo o alerta.

# COMMAND ----------
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..", "src")))
from valueprops_etl import PipelineConfig  # noqa: E402

dbutils.widgets.text("catalog", "valueprops")
cfg = PipelineConfig.from_widgets(dbutils)

# COMMAND ----------
# MAGIC %md ## Última foto de calidad por tabla / expectation

# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("layer", "table", "expectation").orderBy(F.col("checked_at").desc())
latest = (
    spark.table(cfg.audit_table)
    .withColumn("rn", F.row_number().over(w))
    .filter("rn = 1")
    .drop("rn")
    .orderBy("layer", "table", "expectation")
)
display(latest)

# COMMAND ----------
# MAGIC %md ## Gate: ¿alguna expectation crítica ('fail') está en rojo?

# COMMAND ----------
breaches = latest.filter("action = 'fail' AND passed = false")
n = breaches.count()
if n:
    display(breaches)
    raise AssertionError(f"{n} expectations críticas violadas en la última corrida")
print("DQ OK — todas las expectations 'fail' pasaron.")

# COMMAND ----------
# MAGIC %md ## Tendencia de tasa de fallos (para dashboards / alertas)

# COMMAND ----------
trend = (
    spark.table(cfg.audit_table)
    .groupBy("checked_at", "layer", "table")
    .agg(F.round(F.avg("failure_rate") * 100, 3).alias("avg_failure_pct"))
    .orderBy("checked_at")
)
display(trend)
