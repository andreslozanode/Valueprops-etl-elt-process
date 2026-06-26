"""valueprops_etl — pipeline medallón para features de value props (ML-ready)."""

from .config import PipelineConfig
from .quality import DataQualityError, Expectation, apply_expectations

__all__ = [
    "PipelineConfig",
    "Expectation",
    "DataQualityError",
    "apply_expectations",
    "bronze",
    "silver",
    "gold",
    "quality",
    "dq_rules",
]
__version__ = "1.1.0"
