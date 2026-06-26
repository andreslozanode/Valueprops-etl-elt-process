# Databricks notebook source
# MAGIC %md
# MAGIC # Variante NATIVA: DLT / Lakeflow Declarative Pipelines
# MAGIC Mismo medallón, pero con **Expectations declarativas** (`@dlt.expect_*`).
# MAGIC Databricks gestiona el linaje, las métricas de calidad (event log) y la
# MAGIC orquestación incremental automáticamente.
# MAGIC
# MAGIC **Uso**: crea un *DLT pipeline* apuntando a este archivo y configura
# MAGIC `catalog = valueprops`, `landing = /Volumes/valueprops/raw/landing`. No se ejecuta como
# MAGIC notebook normal: lo ejecuta el motor de DLT.

# COMMAND ----------
import dlt
from pyspark.sql import functions as F

LANDING = spark.conf.get("landing", "/Volumes/valueprops/raw/landing")

VALID_VP = (
    "cellphone_recharge", "credits_consumer", "link_cobro",
    "point", "prepaid", "send_money", "transport",
)
_VP_IN = "value_prop IN ('" + "', '".join(VALID_VP) + "')"

# ===========================================================================
# BRONZE — ingesta cruda con Auto Loader (incremental)
# ===========================================================================
@dlt.table(name="prints_raw", comment="Prints crudos + linaje")
def prints_raw():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .load(f"{LANDING}/prints.json")
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )


@dlt.table(name="taps_raw", comment="Taps crudos + linaje")
def taps_raw():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .load(f"{LANDING}/taps.json")
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )


@dlt.table(name="pays_raw", comment="Pagos crudos + linaje")
def pays_raw():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .load(f"{LANDING}/pays.csv")
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )


# ===========================================================================
# SILVER — limpieza + EXPECTATIONS declarativas
# ===========================================================================
_EVENT_EXPECTATIONS = {
    "value_prop_not_null": "value_prop IS NOT NULL",
    "value_prop_known": _VP_IN,
    "position_in_range": "position >= 0 AND position <= 10",
}


@dlt.table(name="prints", partition_cols=["day"])
@dlt.expect_or_fail("user_id_not_null", "user_id IS NOT NULL")   # aborta el update
@dlt.expect_all_or_drop(_EVENT_EXPECTATIONS)                     # descarta inválidas
def prints():
    return (
        dlt.read("prints_raw")
        .select(
            F.to_date("day").alias("day"),
            F.col("user_id").cast("bigint").alias("user_id"),
            F.col("event_data.value_prop").alias("value_prop"),
            F.col("event_data.position").alias("position"),
        )
        .dropDuplicates(["day", "user_id", "value_prop", "position"])
    )


@dlt.table(name="taps", partition_cols=["day"])
@dlt.expect_or_fail("user_id_not_null", "user_id IS NOT NULL")
@dlt.expect_all_or_drop(_EVENT_EXPECTATIONS)
def taps():
    return (
        dlt.read("taps_raw")
        .select(
            F.to_date("day").alias("day"),
            F.col("user_id").cast("bigint").alias("user_id"),
            F.col("event_data.value_prop").alias("value_prop"),
            F.col("event_data.position").alias("position"),
        )
        .dropDuplicates(["day", "user_id", "value_prop", "position"])
    )


@dlt.table(name="pays", partition_cols=["day"])
@dlt.expect_or_fail("user_id_not_null", "user_id IS NOT NULL")
@dlt.expect_or_drop("total_non_negative", "total >= 0")
@dlt.expect("value_prop_known", _VP_IN)                          # warn (retiene)
def pays():
    return dlt.read("pays_raw").select(
        F.to_date("pay_date").alias("day"),
        F.col("user_id").cast("bigint").alias("user_id"),
        F.col("value_prop"),
        F.col("total").cast("double").alias("total"),
    )


# ===========================================================================
# GOLD — feature table con EXPECTATIONS críticas
# ===========================================================================
_GOLD_EXPECTATIONS = {
    "clicked_binary": "clicked IN (0, 1)",
    "counts_non_negative": "views_3w >= 0 AND taps_3w >= 0 AND pays_3w >= 0",
    "amount_non_negative": "amount_3w >= 0",
    "pk_complete": "day IS NOT NULL AND user_id IS NOT NULL "
                   "AND value_prop IS NOT NULL AND position IS NOT NULL",
}


@dlt.table(name="value_prop_features", partition_cols=["day"],
           comment="Feature table ML-ready (última semana de prints)")
@dlt.expect_all_or_fail(_GOLD_EXPECTATIONS)
def value_prop_features():
    prints = dlt.read("prints")
    taps = dlt.read("taps")
    pays = dlt.read("pays")

    max_day = prints.agg(F.max("day").alias("m")).first()["m"]
    base = prints.where(F.col("day") >= F.date_sub(F.lit(max_day), 6)).alias("b")

    events = (
        prints.select("user_id", "value_prop", F.col("day").alias("ev_day"),
                      F.lit("print").alias("etype"), F.lit(0.0).alias("amount"))
        .unionByName(taps.select("user_id", "value_prop", F.col("day").alias("ev_day"),
                                 F.lit("tap").alias("etype"), F.lit(0.0).alias("amount")))
        .unionByName(pays.select("user_id", "value_prop", F.col("day").alias("ev_day"),
                                 F.lit("pay").alias("etype"), F.col("total").alias("amount")))
    ).alias("e")

    clicked = taps.select("day", "user_id", "value_prop").distinct().withColumn("clicked", F.lit(1))
    lo, hi = F.date_sub(F.col("b.day"), 21), F.date_sub(F.col("b.day"), 1)

    agg = (
        base.join(events,
                  (F.col("b.user_id") == F.col("e.user_id"))
                  & (F.col("b.value_prop") == F.col("e.value_prop"))
                  & (F.col("e.ev_day").between(lo, hi)), "left")
        .groupBy("b.day", "b.user_id", "b.value_prop", "b.position")
        .agg(
            F.sum(F.when(F.col("e.etype") == "print", 1).otherwise(0)).alias("views_3w"),
            F.sum(F.when(F.col("e.etype") == "tap", 1).otherwise(0)).alias("taps_3w"),
            F.sum(F.when(F.col("e.etype") == "pay", 1).otherwise(0)).alias("pays_3w"),
            F.coalesce(F.sum(F.when(F.col("e.etype") == "pay", F.col("e.amount"))), F.lit(0.0)).alias("amount_3w"),
        )
    )
    return (
        agg.join(F.broadcast(clicked), ["day", "user_id", "value_prop"], "left")
        .withColumn("clicked", F.coalesce(F.col("clicked"), F.lit(0)))
        .withColumn("ctr_3w", F.when(F.col("views_3w") > 0, F.col("taps_3w") / F.col("views_3w")).otherwise(0.0))
        .withColumn("avg_ticket_3w", F.when(F.col("pays_3w") > 0, F.col("amount_3w") / F.col("pays_3w")).otherwise(0.0))
    )
