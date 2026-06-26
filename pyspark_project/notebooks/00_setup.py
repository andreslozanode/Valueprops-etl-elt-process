# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Setup
# MAGIC Crea widgets de parámetros y deja el paquete `valueprops_etl` importable.
# MAGIC Ejecuta este notebook (o copia la celda de bootstrap) al inicio de cada job.

# COMMAND ----------
dbutils.widgets.text("catalog", "valueprops", "Catálogo UC")
dbutils.widgets.text("landing", "/Volumes/valueprops/raw/landing", "Volumen de aterrizaje")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Bootstrap del paquete
# MAGIC En Databricks Repos, añade la carpeta `src/` al `sys.path` para importar
# MAGIC `valueprops_etl` sin empaquetar. (Alternativa producción: `%pip install -e`.)

# COMMAND ----------
import os
import sys

repo_src = os.path.abspath(os.path.join(os.getcwd(), "..", "src"))
if repo_src not in sys.path:
    sys.path.insert(0, repo_src)

from valueprops_etl import PipelineConfig  # noqa: E402

cfg = PipelineConfig.from_widgets(dbutils)
print("Config:", cfg)
