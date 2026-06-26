"""Capa GOLD: tabla de features ML-ready para los prints de la última semana.

Para cada print de la última semana se calcula:
  * clicked        -> label binaria (¿el usuario tapeó ese print?)
  * views_3w       -> # de veces que vio ese value_prop en las 3 semanas previas
  * taps_3w        -> # de veces que tapeó ese value_prop en las 3 semanas previas
  * pays_3w        -> # de pagos de ese value_prop en las 3 semanas previas
  * amount_3w      -> monto acumulado pagado de ese value_prop en las 3 semanas previas

OPTIMIZACIÓN CLAVE — un solo range-join:
  En lugar de 3 joins de intervalo (prints, taps, pays) hacemos UNA unión de
  eventos tipados y UN range-join contra los prints base. Esto reduce de 3
  shuffles costosos a 1, y la agregación condicional (sum + when) deriva las
  4 métricas en una sola pasada. Photon + AQE manejan el skew.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .common import optimize_zorder, write_delta
from .config import PipelineConfig
from .dq_rules import GOLD_EXPECTATIONS
from .quality import apply_expectations

# Clave natural de un print (= primary key de la feature table para el Feature Store)
PK = ["day", "user_id", "value_prop", "position"]


def compute_features(
    prints: DataFrame,
    taps: DataFrame,
    pays: DataFrame,
    feature_window_days: int = 21,
    serving_window_days: int = 7,
) -> DataFrame:
    """Función PURA: deriva la feature table a partir de DataFrames Silver.

    Separada de la E/S para ser testeable de forma unitaria sin metastore.
    """
    max_day = prints.agg(F.max("day").alias("m")).first()["m"]
    base = (
        prints.where(
            F.col("day") >= F.date_sub(F.lit(max_day), serving_window_days - 1)
        )
        .dropDuplicates(
            PK
        )  # un print es único por (day, user_id, value_prop, position)
        .alias("b")
    )

    events = (
        prints.select(
            "user_id",
            "value_prop",
            F.col("day").alias("ev_day"),
            F.lit("print").alias("etype"),
            F.lit(0.0).alias("amount"),
        )
        .unionByName(
            taps.select(
                "user_id",
                "value_prop",
                F.col("day").alias("ev_day"),
                F.lit("tap").alias("etype"),
                F.lit(0.0).alias("amount"),
            )
        )
        .unionByName(
            pays.select(
                "user_id",
                "value_prop",
                F.col("day").alias("ev_day"),
                F.lit("pay").alias("etype"),
                F.col("total").alias("amount"),
            )
        )
    ).alias("e")

    # Label: ¿este print fue tapeado? broadcast del lookup (tabla pequeña).
    clicked = (
        taps.select("day", "user_id", "value_prop")
        .distinct()
        .withColumn("clicked", F.lit(1))
    )

    win_lo = F.date_sub(F.col("b.day"), feature_window_days)  # day - 21
    win_hi = F.date_sub(F.col("b.day"), 1)  # day - 1

    # --- ÚNICO range-join: eventos en [day-21, day-1] del mismo user+value_prop
    joined = base.join(
        events,
        on=(
            (F.col("b.user_id") == F.col("e.user_id"))
            & (F.col("b.value_prop") == F.col("e.value_prop"))
            & (F.col("e.ev_day").between(win_lo, win_hi))
        ),
        how="left",
    )

    agg = joined.groupBy([F.col(f"b.{c}").alias(c) for c in PK]).agg(
        F.sum(F.when(F.col("e.etype") == "print", 1).otherwise(0)).alias("views_3w"),
        F.sum(F.when(F.col("e.etype") == "tap", 1).otherwise(0)).alias("taps_3w"),
        F.sum(F.when(F.col("e.etype") == "pay", 1).otherwise(0)).alias("pays_3w"),
        F.coalesce(
            F.sum(F.when(F.col("e.etype") == "pay", F.col("e.amount"))), F.lit(0.0)
        ).alias("amount_3w"),
    )

    features = (
        agg.join(F.broadcast(clicked), on=["day", "user_id", "value_prop"], how="left")
        .withColumn("clicked", F.coalesce(F.col("clicked"), F.lit(0)))
        # features derivadas listas para ML
        .withColumn(
            "ctr_3w",
            F.when(
                F.col("views_3w") > 0, F.col("taps_3w") / F.col("views_3w")
            ).otherwise(0.0),
        )
        .withColumn(
            "avg_ticket_3w",
            F.when(
                F.col("pays_3w") > 0, F.col("amount_3w") / F.col("pays_3w")
            ).otherwise(0.0),
        )
        .withColumn("_computed_at", F.current_timestamp())
        .select(
            "day",
            "user_id",
            "value_prop",
            "position",
            "clicked",  # <- label
            "views_3w",
            "taps_3w",
            "pays_3w",
            "amount_3w",
            "ctr_3w",
            "avg_ticket_3w",
            "_computed_at",
        )
    )
    return features


def build_features(spark: SparkSession, cfg: PipelineConfig) -> DataFrame:
    """Lee las tablas Silver y delega en la función pura compute_features."""
    return compute_features(
        prints=spark.table(cfg.s("prints")),
        taps=spark.table(cfg.s("taps")),
        pays=spark.table(cfg.s("pays")),
        feature_window_days=cfg.feature_window_days,
        serving_window_days=cfg.serving_window_days,
    )


def run(spark: SparkSession, cfg: PipelineConfig) -> None:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.catalog}.{cfg.gold_schema}")
    features = build_features(spark, cfg)

    # Gate de calidad: valida la feature table ANTES de publicarla para ML.
    features = apply_expectations(
        spark,
        features,
        GOLD_EXPECTATIONS,
        "gold",
        "value_prop_features",
        cfg.audit_table,
    )

    write_delta(features, cfg.g("value_prop_features"), partition_by=["day"])
    optimize_zorder(spark, cfg.g("value_prop_features"), ["user_id", "value_prop"])

    # Constraints de calidad: el Gold debe ser confiable para entrenar modelos.
    tbl = cfg.g("value_prop_features")
    spark.sql(f"ALTER TABLE {tbl} ALTER COLUMN user_id SET NOT NULL")
    spark.sql(f"ALTER TABLE {tbl} ALTER COLUMN value_prop SET NOT NULL")
    spark.sql(
        f"ALTER TABLE {tbl} ADD CONSTRAINT clicked_binary CHECK (clicked IN (0,1))"
    )
