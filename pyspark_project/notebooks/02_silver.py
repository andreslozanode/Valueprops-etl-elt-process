# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Silver — Limpieza, tipado y particionado
# MAGIC Aplana `event_data`, castea fechas a DATE, deduplica y particiona por
# MAGIC `day`. Aplica OPTIMIZE ZORDER(user_id, value_prop).

# COMMAND ----------
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..", "src")))
from valueprops_etl import PipelineConfig, silver  # noqa: E402

dbutils.widgets.text("catalog", "valueprops")
dbutils.widgets.text("landing", "/Volumes/valueprops/raw/landing")
cfg = PipelineConfig.from_widgets(dbutils)

# COMMAND ----------
silver.run(spark, cfg)

# COMMAND ----------
display(spark.table(cfg.s("prints")).limit(5))
