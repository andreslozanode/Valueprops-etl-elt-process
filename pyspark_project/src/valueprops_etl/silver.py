"""Capa SILVER: limpia, tipa, aplana y deduplica. Particionada por fecha.

Transformaciones:
* aplana event_data.{position, value_prop},
* castea day/pay_date a DATE (evita comparaciones de string en el Gold),
* aplica EXPECTATIONS (calidad) como gate antes de escribir,
* deduplica por clave natural,
* particiona por la fecha del evento -> habilita *partition pruning* en la
  ventana de 3 semanas del Gold.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .common import optimize_zorder, write_delta
from .config import PipelineConfig
from .dq_rules import EVENT_EXPECTATIONS, PAYS_EXPECTATIONS
from .quality import apply_expectations


def _flatten_event(df: DataFrame) -> DataFrame:
    return df.select(
        F.to_date("day").alias("day"),
        F.col("user_id"),
        F.col("event_data.value_prop").alias("value_prop"),
        F.col("event_data.position").alias("position"),
    )


def build_prints(spark: SparkSession, cfg: PipelineConfig) -> DataFrame:
    df = _flatten_event(spark.table(cfg.b("prints_raw")))
    df = apply_expectations(
        spark, df, EVENT_EXPECTATIONS, "silver", "prints", cfg.audit_table
    ).dropDuplicates(["day", "user_id", "value_prop", "position"])
    write_delta(df, cfg.s("prints"), partition_by=["day"])
    optimize_zorder(spark, cfg.s("prints"), ["user_id", "value_prop"])
    return df


def build_taps(spark: SparkSession, cfg: PipelineConfig) -> DataFrame:
    df = _flatten_event(spark.table(cfg.b("taps_raw")))
    df = apply_expectations(
        spark, df, EVENT_EXPECTATIONS, "silver", "taps", cfg.audit_table
    ).dropDuplicates(["day", "user_id", "value_prop", "position"])
    write_delta(df, cfg.s("taps"), partition_by=["day"])
    optimize_zorder(spark, cfg.s("taps"), ["user_id", "value_prop"])
    return df


def build_pays(spark: SparkSession, cfg: PipelineConfig) -> DataFrame:
    df = spark.table(cfg.b("pays_raw")).select(
        F.to_date("pay_date").alias("day"),
        F.col("user_id"),
        F.col("value_prop"),
        F.col("total").cast("double").alias("total"),
    )
    df = apply_expectations(
        spark, df, PAYS_EXPECTATIONS, "silver", "pays", cfg.audit_table
    )
    write_delta(df, cfg.s("pays"), partition_by=["day"])
    optimize_zorder(spark, cfg.s("pays"), ["user_id", "value_prop"])
    return df


def run(spark: SparkSession, cfg: PipelineConfig) -> None:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.catalog}.{cfg.silver_schema}")
    build_prints(spark, cfg)
    build_taps(spark, cfg)
    build_pays(spark, cfg)
