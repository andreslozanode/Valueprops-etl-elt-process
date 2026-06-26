"""Utilidades compartidas: esquemas explícitos, escritura Delta y mantenimiento.

Decisiones de rendimiento clave:
* Esquemas EXPLÍCITOS al leer JSON/CSV  -> evita un job extra de inferencia
  sobre 500k+ líneas y previene cambios de tipo silenciosos.
* Escritura Delta con optimizeWrite/autoCompact -> evita el "small files problem".
* Sin UDFs de Python en todo el pipeline -> compatible con Photon y vectorizado.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

# --- Esquemas explícitos de la zona de aterrizaje ---------------------------
PRINTS_TAPS_SCHEMA = StructType(
    [
        StructField("day", StringType(), True),
        StructField(
            "event_data",
            StructType(
                [
                    StructField("position", IntegerType(), True),
                    StructField("value_prop", StringType(), True),
                ]
            ),
            True,
        ),
        StructField("user_id", LongType(), True),
    ]
)

PAYS_SCHEMA = StructType(
    [
        StructField("pay_date", StringType(), True),
        StructField("total", DoubleType(), True),
        StructField("user_id", LongType(), True),
        StructField("value_prop", StringType(), True),
    ]
)


def write_delta(
    df: DataFrame,
    table: str,
    partition_by: list[str] | None = None,
    mode: str = "overwrite",
) -> None:
    """Escribe un DataFrame como tabla Delta gestionada en Unity Catalog.

    autoOptimize via table properties: compacta y ordena en cada escritura,
    eliminando la necesidad de OPTIMIZE manual frecuente.
    """
    writer = (
        df.write.format("delta")
        .mode(mode)
        .option("overwriteSchema", "true")
        .option("delta.autoOptimize.optimizeWrite", "true")
        .option("delta.autoOptimize.autoCompact", "true")
    )
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.saveAsTable(table)


def optimize_zorder(spark: SparkSession, table: str, zorder_cols: list[str]) -> None:
    """Compacta y co-localiza datos por las columnas de filtro/join frecuentes.

    ZORDER sobre (user_id, value_prop) acelera el range-join del Gold por
    data-skipping. En DBR 15.2+ se puede sustituir por Liquid Clustering
    (CLUSTER BY) que no requiere re-especificar columnas al consultar.
    """
    cols = ", ".join(zorder_cols)
    spark.sql(f"OPTIMIZE {table} ZORDER BY ({cols})")


def vacuum(spark: SparkSession, table: str, retention_hours: int = 168) -> None:
    """Elimina archivos de versiones antiguas (default 7 días) para liberar costo."""
    spark.sql(f"VACUUM {table} RETAIN {retention_hours} HOURS")
