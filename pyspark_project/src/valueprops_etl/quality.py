"""Motor portable de *Expectations* (calidad de datos) con semántica tipo DLT.

Replica las 3 acciones de Delta Live Tables / Lakeflow en jobs CLÁSICOS:
    * ``warn`` : registra la violación pero deja pasar la fila (observabilidad).
    * ``drop`` : descarta las filas que violan la regla (cuarentena opcional).
    * ``fail`` : aborta el pipeline si CUALQUIER fila viola la regla.

Diseño:
* Las reglas son expresiones SQL booleanas que deben ser TRUE en filas válidas.
* Se evalúan en UNA sola pasada (agregación condicional) -> Photon-friendly, sin UDFs.
* Una expresión que evalúa a NULL se trata como VIOLACIÓN (regla estricta de DQ).
* Los resultados se persisten en una tabla de auditoría Delta para monitoreo.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

VALID_ACTIONS = {"warn", "drop", "fail"}


class DataQualityError(Exception):
    """Se lanza cuando una expectation de acción ``fail`` es violada."""


@dataclass(frozen=True)
class Expectation:
    name: str
    constraint: str  # expresión SQL booleana: TRUE = fila válida
    action: str = "warn"  # "warn" | "drop" | "fail"

    def __post_init__(self) -> None:
        if self.action not in VALID_ACTIONS:
            raise ValueError(f"Acción inválida {self.action!r}; usa {VALID_ACTIONS}")


def _safe(constraint: str) -> str:
    # NULL -> False, de modo que las filas con NULL cuenten como violación.
    return f"coalesce(({constraint}), false)"


def evaluate(df: DataFrame, expectations: list[Expectation]) -> tuple[dict, int]:
    """Devuelve ({nombre: filas_violadas}, total_filas) en una sola pasada."""
    total_col = F.count(F.lit(1)).alias("_total")
    viol_cols = [
        F.sum(F.when(~F.expr(_safe(e.constraint)), 1).otherwise(0)).alias(e.name)
        for e in expectations
    ]
    row = df.agg(total_col, *viol_cols).first().asDict()
    total = row.pop("_total")
    return row, total


def apply_expectations(
    spark: SparkSession,
    df: DataFrame,
    expectations: list[Expectation],
    layer: str,
    table: str,
    audit_table: str | None = None,
    run_id: str | None = None,
) -> DataFrame:
    """Evalúa, registra auditoría, aplica drops y aborta en fallos ``fail``.

    Retorna el DataFrame limpio (sin las filas descartadas por reglas ``drop``).
    """
    run_id = run_id or str(uuid.uuid4())
    df = df.cache()  # se reutiliza para conteo y para el filtrado de drops
    violations, total = evaluate(df, expectations)

    by_name = {e.name: e for e in expectations}
    results = []
    for name, failed in violations.items():
        e = by_name[name]
        rate = (failed / total) if total else 0.0
        results.append(
            {
                "run_id": run_id,
                "layer": layer,
                "table": table,
                "expectation": name,
                "constraint": e.constraint,
                "action": e.action,
                "total_rows": int(total),
                "failed_rows": int(failed),
                "failure_rate": float(rate),
                "passed": failed == 0,
                "checked_at": datetime.now(timezone.utc),
            }
        )

    _log(spark, results, audit_table)
    _print_summary(table, results)

    # 1) FAIL: aborta si alguna regla crítica se viola.
    fail_violations = [r for r in results if r["action"] == "fail" and not r["passed"]]
    if fail_violations:
        detail = ", ".join(
            f"{r['expectation']}={r['failed_rows']}" for r in fail_violations
        )
        raise DataQualityError(f"[{table}] expectations 'fail' violadas: {detail}")

    # 2) DROP: conserva solo las filas que cumplen TODAS las reglas 'drop'.
    drop_rules = [e for e in expectations if e.action == "drop"]
    if drop_rules:
        keep = " AND ".join(_safe(e.constraint) for e in drop_rules)
        df = df.where(F.expr(keep))

    return df


def _log(spark: SparkSession, results: list[dict], audit_table: str | None) -> None:
    if not audit_table or not results:
        return
    schema = audit_table.rsplit(".", 1)[0]
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    (
        spark.createDataFrame(results)
        .write.format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(audit_table)
    )


def _print_summary(table: str, results: list[dict]) -> None:
    print(f"\n── Data Quality · {table} ─────────────────────────────")
    for r in results:
        flag = "OK " if r["passed"] else "XX "
        print(
            f"  {flag}[{r['action']:>4}] {r['expectation']:<28} "
            f"violadas={r['failed_rows']:>6} ({r['failure_rate']*100:5.2f}%)"
        )
