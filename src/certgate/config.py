"""Configuration dataclasses that describe the CertGate pipeline targets and thresholds."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Mapping, Sequence, Tuple

from certgate.ingest.loaders import LoadedTable
from certgate.rules.schema import RuleOutcome, SCHEMA_TARGETS

TablesMapping = Mapping[str, LoadedTable]
BusinessRuleCallable = Callable[[TablesMapping], Sequence[RuleOutcome]]


@dataclass(frozen=True)
class SchemaTargetConfig:
    name: str
    dtype_rule_prefix: str | None = None
    uniqueness_rule_prefix: str | None = None


@dataclass(frozen=True)
class FreshnessConfig:
    timestamp_column: str | None = "file_received_ts"
    metadata_timestamp_keys: tuple[str, ...] = (
        "file_received_ts",
        "ingest_time",
        "status_effective_ts",
    )
    max_allowed_hours: int = 24
    warning_hours: int | None = None
    rule_id: str = "BR-08"


@dataclass(frozen=True)
class BusinessRuleConfig:
    name: str
    rule: BusinessRuleCallable


@dataclass(frozen=True)
class PipelineConfig:
    data_root: Path = Path("data")
    bundle: str = "good"
    reports_dir: Path = Path("reports")
    schema_targets: Tuple[SchemaTargetConfig, ...] = field(
        default_factory=lambda: tuple(
            SchemaTargetConfig(name=target) for target in SCHEMA_TARGETS
        )
    )
    freshness: FreshnessConfig = field(default_factory=FreshnessConfig)
    business_rules: Tuple[BusinessRuleConfig, ...] = field(default_factory=tuple)

__all__ = [
    "SchemaTargetConfig",
    "FreshnessConfig",
    "BusinessRuleConfig",
    "PipelineConfig",
    "BusinessRuleCallable",
    "TablesMapping",
]
