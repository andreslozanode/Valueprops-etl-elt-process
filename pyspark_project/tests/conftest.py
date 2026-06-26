"""Fixtures de test: SparkSession local ligera."""

import os
import sys

import pytest
from pyspark.sql import SparkSession

# Hace importable el paquete src/ sin instalar
sys.path.insert(
    0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src"))
)


@pytest.fixture(scope="session")
def spark():
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("valueprops-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    yield spark
    spark.stop()
