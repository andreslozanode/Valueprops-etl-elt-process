"""Configuración tipada del pipeline (medallón Bronze -> Silver -> Gold).

Centraliza nombres de catálogo/esquema/tabla y parámetros de negocio para que
los notebooks sean *thin wrappers* sin reglas hardcodeadas.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class PipelineConfig:
    # --- Unity Catalog (namespace de 3 niveles: catalog.schema.table) ---
    catalog: str = "valueprops"
    bronze_schema: str = "bronze"
    silver_schema: str = "silver"
    gold_schema: str = "gold"
    monitoring_schema: str = "monitoring"

    # --- Rutas de aterrizaje de datos crudos (Volume UC recomendado) ---
    landing: str = "/Volumes/valueprops/raw/landing"

    # --- Parámetros de negocio (ver docs/assumptions.md) ---
    feature_window_days: int = 21  # "3 semanas previas" al print
    serving_window_days: int = 7  # "última semana" de prints a servir

    # ---- Helpers de nombres totalmente calificados ----
    def b(self, table: str) -> str:
        return f"{self.catalog}.{self.bronze_schema}.{table}"

    def s(self, table: str) -> str:
        return f"{self.catalog}.{self.silver_schema}.{table}"

    def g(self, table: str) -> str:
        return f"{self.catalog}.{self.gold_schema}.{table}"

    @property
    def audit_table(self) -> str:
        return f"{self.catalog}.{self.monitoring_schema}.dq_results"

    @property
    def prints_path(self) -> str:
        return f"{self.landing}/prints.json"

    @property
    def taps_path(self) -> str:
        return f"{self.landing}/taps.json"

    @property
    def pays_path(self) -> str:
        return f"{self.landing}/pays.csv"

    @classmethod
    def from_widgets(cls, dbutils=None, **overrides) -> "PipelineConfig":
        """Construye la config desde widgets de Databricks si están presentes."""
        kwargs = {}
        if dbutils is not None:
            for field_name in ("catalog", "landing"):
                try:
                    val = dbutils.widgets.get(field_name)
                    if val:
                        kwargs[field_name] = val
                except Exception:
                    pass
        kwargs.update(overrides)
        return cls(**kwargs)
