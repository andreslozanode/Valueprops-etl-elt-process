# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Gold — Feature table ML-ready
# MAGIC Calcula features de la última semana de prints con UN range-join sobre
# MAGIC eventos unificados. Escribe `gold.value_prop_features` con constraints.

# COMMAND ----------
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..", "src")))
from valueprops_etl import PipelineConfig, gold  # noqa: E402

dbutils.widgets.text("catalog", "valueprops")
dbutils.widgets.text("landing", "/Volumes/valueprops/raw/landing")
cfg = PipelineConfig.from_widgets(dbutils)

# COMMAND ----------
gold.run(spark, cfg)

# COMMAND ----------
# MAGIC %md ### Validación rápida
# COMMAND ----------
df = spark.table(cfg.g("value_prop_features"))
print("Filas:", df.count())
display(df.orderBy("views_3w", ascending=False).limit(10))
