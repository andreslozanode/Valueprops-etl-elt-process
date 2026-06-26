"""Catálogo de expectations por capa. Editar reglas aquí, no en la lógica ETL."""

from __future__ import annotations

from .quality import Expectation

# Categorías válidas observadas en las 3 fuentes (ver docs/data_dictionary.md).
VALID_VALUE_PROPS = (
    "cellphone_recharge",
    "credits_consumer",
    "link_cobro",
    "point",
    "prepaid",
    "send_money",
    "transport",
)
_VP_IN = "value_prop IN ('" + "', '".join(VALID_VALUE_PROPS) + "')"


# --- Eventos de prints / taps (Silver) -------------------------------------
EVENT_EXPECTATIONS: list[Expectation] = [
    Expectation("user_id_not_null", "user_id IS NOT NULL", "fail"),
    Expectation("value_prop_not_null", "value_prop IS NOT NULL", "drop"),
    Expectation("value_prop_known", _VP_IN, "warn"),
    Expectation("day_not_null", "day IS NOT NULL", "fail"),
    Expectation("day_not_future", "day <= current_date()", "warn"),
    Expectation("position_in_range", "position >= 0 AND position <= 10", "warn"),
]

# --- Pagos (Silver) --------------------------------------------------------
PAYS_EXPECTATIONS: list[Expectation] = [
    Expectation("user_id_not_null", "user_id IS NOT NULL", "fail"),
    Expectation("value_prop_known", _VP_IN, "warn"),
    Expectation("day_not_null", "day IS NOT NULL", "fail"),
    Expectation("total_non_negative", "total >= 0", "drop"),
]

# --- Feature table (Gold) --------------------------------------------------
GOLD_EXPECTATIONS: list[Expectation] = [
    Expectation("clicked_binary", "clicked IN (0, 1)", "fail"),
    Expectation(
        "counts_non_negative", "views_3w >= 0 AND taps_3w >= 0 AND pays_3w >= 0", "fail"
    ),
    Expectation("amount_non_negative", "amount_3w >= 0", "fail"),
    # Consistencia: sin pagos -> monto 0; con pagos -> monto > 0.
    Expectation(
        "amount_matches_pays",
        "(pays_3w = 0 AND amount_3w = 0) OR (pays_3w > 0)",
        "warn",
    ),
    Expectation("ctr_bounded", "ctr_3w >= 0 AND ctr_3w <= 1", "warn"),
    Expectation(
        "pk_complete",
        "day IS NOT NULL AND user_id IS NOT NULL AND value_prop IS NOT NULL "
        "AND position IS NOT NULL",
        "fail",
    ),
]
