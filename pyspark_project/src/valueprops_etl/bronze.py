"""Capa BRONZE: ingesta cruda fiel a la fuente + metadatos de linaje.

Regla del medallón: Bronze NO transforma reglas de negocio. Solo:
* lee con esquema explícito,
* añade columnas de auditoría (_ingested_at, _source_file),
* persiste en Delta para tener una base reproducible e idempotente.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .common import PAYS_SCHEMA, PRINTS_TAPS_SCHEMA, write_delta
from .config import PipelineConfig


def _with_audit(df: DataFrame) -> DataFrame:
    return df.withColumn("_ingested_at", F.current_timestamp()).withColumn(
        "_source_file", F.col("_metadata.file_path")
    )


def ingest_prints(spark: SparkSession, cfg: PipelineConfig) -> DataFrame:
    df = (
        spark.read.schema(PRINTS_TAPS_SCHEMA)
        .json(cfg.prints_path)
        .transform(_with_audit)
    )
    write_delta(df, cfg.b("prints_raw"))
    return df


def ingest_taps(spark: SparkSession, cfg: PipelineConfig) -> DataFrame:
    df = (
        spark.read.schema(PRINTS_TAPS_SCHEMA).json(cfg.taps_path).transform(_with_audit)
    )
    write_delta(df, cfg.b("taps_raw"))
    return df


def ingest_pays(spark: SparkSession, cfg: PipelineConfig) -> DataFrame:
    df = (
        spark.read.schema(PAYS_SCHEMA)
        .option("header", "true")
        .csv(cfg.pays_path)
        .transform(_with_audit)
    )
    write_delta(df, cfg.b("pays_raw"))
    return df


def run(spark: SparkSession, cfg: PipelineConfig) -> None:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {cfg.catalog}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.catalog}.{cfg.bronze_schema}")
    ingest_prints(spark, cfg)
    ingest_taps(spark, cfg)
    ingest_pays(spark, cfg)
