# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Consumo ML/AI — Feature Engineering in Unity Catalog + MLflow
# MAGIC Demuestra que el Gold está listo para ML:
# MAGIC 1. Registra `gold.value_prop_features` como **Feature Table** (UC).
# MAGIC 2. Crea un **training set** con lookup por clave primaria.
# MAGIC 3. Entrena un baseline (predicción de `clicked`) y lo loguea en **MLflow**.
# MAGIC 4. Deja la tabla lista para **online serving** / batch scoring.

# COMMAND ----------
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..", "src")))
from valueprops_etl import PipelineConfig  # noqa: E402

dbutils.widgets.text("catalog", "valueprops")
cfg = PipelineConfig.from_widgets(dbutils)
feature_table = cfg.g("value_prop_features")

# COMMAND ----------
# MAGIC %md ## 1. Registrar la Feature Table en Unity Catalog
# MAGIC La PK `(day, user_id, value_prop, position)` permite lookups deterministas.

# COMMAND ----------
from databricks.feature_engineering import FeatureEngineeringClient

fe = FeatureEngineeringClient()

# Idempotente: si ya existe como tabla Delta gestionada con PK, solo la referenciamos.
spark.sql(f"""
    ALTER TABLE {feature_table}
    ADD CONSTRAINT vp_pk PRIMARY KEY (day, user_id, value_prop, position)
""")  # si ya existe la constraint, ignora el error en re-ejecuciones

# COMMAND ----------
# MAGIC %md ## 2. Construir el training set vía FeatureLookup

# COMMAND ----------
from databricks.feature_engineering import FeatureLookup

# "spine": las claves + el label. Las features se traen por lookup desde la FT.
spine = spark.table(feature_table).select(
    "day", "user_id", "value_prop", "position", "clicked"
)

training_set = fe.create_training_set(
    df=spine,
    feature_lookups=[
        FeatureLookup(
            table_name=feature_table,
            lookup_key=["day", "user_id", "value_prop", "position"],
            feature_names=[
                "views_3w", "taps_3w", "pays_3w",
                "amount_3w", "ctr_3w", "avg_ticket_3w",
            ],
        )
    ],
    label="clicked",
)
training_df = training_set.load_df()

# COMMAND ----------
# MAGIC %md ## 3. Entrenar baseline y loguear con MLflow

# COMMAND ----------
import mlflow
from pyspark.ml import Pipeline
from pyspark.ml.classification import GBTClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import StringIndexer, VectorAssembler

train, test = training_df.randomSplit([0.8, 0.2], seed=42)

idx = StringIndexer(inputCol="value_prop", outputCol="value_prop_idx", handleInvalid="keep")
features = ["views_3w", "taps_3w", "pays_3w", "amount_3w", "ctr_3w", "avg_ticket_3w", "value_prop_idx", "position"]
assembler = VectorAssembler(inputCols=features, outputCol="features")
gbt = GBTClassifier(labelCol="clicked", featuresCol="features", maxIter=40)

mlflow.set_registry_uri("databricks-uc")
with mlflow.start_run(run_name="valueprop_click_baseline") as run:
    model = Pipeline(stages=[idx, assembler, gbt]).fit(train)
    preds = model.transform(test)
    auc = BinaryClassificationEvaluator(labelCol="clicked", metricName="areaUnderROC").evaluate(preds)
    mlflow.log_metric("auc", auc)
    print(f"AUC test: {auc:.4f}")

    # Loguear con linaje a la Feature Table -> reproducible y servible online
    fe.log_model(
        model=model,
        artifact_path="model",
        flavor=mlflow.spark,
        training_set=training_set,
        registered_model_name=f"{cfg.catalog}.{cfg.gold_schema}.valueprop_click_model",
    )

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Scoring batch reproducible
# MAGIC `fe.score_batch` re-une automáticamente las features por las claves,
# MAGIC garantizando que entrenamiento y serving usen exactamente la misma lógica
# MAGIC (evita *training/serving skew*).

# COMMAND ----------
# scored = fe.score_batch(
#     model_uri=f"models:/{cfg.catalog}.{cfg.gold_schema}.valueprop_click_model@champion",
#     df=spine.drop("clicked"),
# )
# display(scored.select("user_id", "value_prop", "prediction"))
