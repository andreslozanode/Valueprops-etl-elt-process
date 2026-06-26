# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Bronze — Ingesta cruda
# MAGIC Lee prints/taps/pays con esquema explícito y persiste en Delta con
# MAGIC metadatos de linaje. Idempotente (overwrite).

# COMMAND ----------
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..", "src")))
from valueprops_etl import PipelineConfig, bronze  # noqa: E402

dbutils.widgets.text("catalog", "valueprops")
dbutils.widgets.text("landing", "/Volumes/valueprops/raw/landing")
cfg = PipelineConfig.from_widgets(dbutils)

# COMMAND ----------
bronze.run(spark, cfg)

# COMMAND ----------
display(spark.table(cfg.b("prints_raw")).limit(5))
