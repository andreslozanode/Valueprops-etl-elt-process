"""Tests del motor de Expectations (warn / drop / fail)."""

import pytest
from pyspark.sql import Row

from valueprops_etl.quality import (
    DataQualityError,
    Expectation,
    apply_expectations,
    evaluate,
)


def _df(spark):
    return spark.createDataFrame(
        [
            Row(user_id=1, value_prop="point", total=10.0),
            Row(user_id=2, value_prop="point", total=-5.0),  # total negativo
            Row(user_id=None, value_prop="prepaid", total=3.0),  # user_id null
            Row(user_id=4, value_prop=None, total=1.0),  # value_prop null
        ]
    )


def test_evaluate_counts_violations_and_nulls(spark):
    exps = [
        Expectation("user_not_null", "user_id IS NOT NULL", "warn"),
        Expectation("total_non_negative", "total >= 0", "warn"),
        Expectation("vp_not_null", "value_prop IS NOT NULL", "warn"),
    ]
    viol, total = evaluate(_df(spark), exps)
    assert total == 4
    assert viol["user_not_null"] == 1  # 1 null
    assert viol["total_non_negative"] == 1  # 1 negativo
    assert viol["vp_not_null"] == 1  # 1 null


def test_drop_removes_failing_rows(spark):
    exps = [Expectation("total_non_negative", "total >= 0", "drop")]
    out = apply_expectations(spark, _df(spark), exps, "test", "t")
    assert out.count() == 3  # se descarta la fila con total -5
    assert out.filter("total < 0").count() == 0


def test_fail_raises_on_violation(spark):
    exps = [Expectation("user_not_null", "user_id IS NOT NULL", "fail")]
    with pytest.raises(DataQualityError):
        apply_expectations(spark, _df(spark), exps, "test", "t")


def test_warn_keeps_all_rows(spark):
    exps = [Expectation("total_non_negative", "total >= 0", "warn")]
    out = apply_expectations(spark, _df(spark), exps, "test", "t")
    assert out.count() == 4  # warn no descarta nada


def test_clean_data_passes_fail_rule(spark):
    clean = spark.createDataFrame([Row(user_id=1, value_prop="point", total=1.0)])
    exps = [Expectation("user_not_null", "user_id IS NOT NULL", "fail")]
    out = apply_expectations(spark, clean, exps, "test", "t")
    assert out.count() == 1


def test_invalid_action_rejected():
    with pytest.raises(ValueError):
        Expectation("x", "1=1", "explode")
