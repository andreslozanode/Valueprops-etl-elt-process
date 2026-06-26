"""Tests unitarios de la lógica de features (función pura, sin metastore)."""

import datetime as dt

from pyspark.sql import Row

from valueprops_etl.gold import compute_features


def _df(spark, rows):
    return spark.createDataFrame(rows)


def test_feature_window_excludes_print_day_and_old_events(spark):
    """Solo cuentan eventos en [day-21, day-1]; el día del print NO cuenta."""
    D = dt.date(2020, 11, 30)
    prints = _df(
        spark,
        [
            Row(day=D, position=0, value_prop="point", user_id=1),  # print servido
            Row(
                day=D - dt.timedelta(days=1), position=0, value_prop="point", user_id=1
            ),  # cuenta
            Row(
                day=D - dt.timedelta(days=21), position=0, value_prop="point", user_id=1
            ),  # cuenta (borde)
            Row(
                day=D - dt.timedelta(days=22), position=0, value_prop="point", user_id=1
            ),  # NO cuenta (>21)
            Row(
                day=D, position=0, value_prop="point", user_id=1
            ),  # mismo día: NO cuenta como histórico
        ],
    )
    taps = _df(spark, [Row(day=D, position=0, value_prop="point", user_id=1)])
    pays = _df(
        spark,
        [Row(day=D - dt.timedelta(days=5), value_prop="point", user_id=1, total=10.0)],
    )

    out = compute_features(prints, taps, pays)
    r = out.filter(out.day == D).first()  # el print servido de hoy (D)
    assert r["views_3w"] == 2  # day-1 y day-21
    assert r["taps_3w"] == 0  # no hubo taps en la ventana previa
    assert r["pays_3w"] == 1
    assert r["amount_3w"] == 10.0
    assert r["clicked"] == 1  # tapeado el día del print


def test_clicked_flag_zero_when_no_tap(spark):
    D = dt.date(2020, 11, 30)
    prints = _df(spark, [Row(day=D, position=1, value_prop="prepaid", user_id=7)])
    taps = _df(
        spark, [Row(day=D, position=0, value_prop="point", user_id=7)]
    )  # otro value_prop
    pays = _df(spark, [Row(day=D, value_prop="prepaid", user_id=7, total=5.0)])

    r = compute_features(prints, taps, pays).first()
    assert r["clicked"] == 0
    assert r["amount_3w"] == 0.0  # el pago es del mismo día -> fuera de ventana


def test_derived_features_ctr_and_ticket(spark):
    D = dt.date(2020, 11, 30)
    prev = D - dt.timedelta(days=3)
    prints = _df(
        spark,
        [
            Row(day=D, position=0, value_prop="transport", user_id=9),
            Row(day=prev, position=0, value_prop="transport", user_id=9),
            Row(day=prev, position=0, value_prop="transport", user_id=9),
        ],
    )
    taps = _df(spark, [Row(day=prev, position=0, value_prop="transport", user_id=9)])
    pays = _df(
        spark,
        [
            Row(day=prev, value_prop="transport", user_id=9, total=100.0),
            Row(day=prev, value_prop="transport", user_id=9, total=50.0),
        ],
    )
    r = compute_features(prints, taps, pays).filter("day = '2020-11-30'").first()
    assert r["views_3w"] == 2
    assert r["taps_3w"] == 1
    assert abs(r["ctr_3w"] - 0.5) < 1e-9
    assert r["pays_3w"] == 2
    assert abs(r["avg_ticket_3w"] - 75.0) < 1e-9


def test_only_last_week_is_served(spark):
    """Prints fuera de la última semana no aparecen en el Gold."""
    last = dt.date(2020, 11, 30)
    prints = _df(
        spark,
        [
            Row(day=last, position=0, value_prop="point", user_id=1),
            Row(
                day=last - dt.timedelta(days=10),
                position=0,
                value_prop="point",
                user_id=2,
            ),  # fuera
        ],
    )
    taps = _df(spark, [Row(day=last, position=0, value_prop="point", user_id=1)])
    pays = _df(spark, [Row(day=last, value_prop="point", user_id=1, total=1.0)])

    users = {r["user_id"] for r in compute_features(prints, taps, pays).collect()}
    assert users == {1}
