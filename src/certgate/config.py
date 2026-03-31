"""Configuration models for the CertGate CRM integrity pipeline."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
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
    null_rule_prefix: str | None = None


@dataclass(frozen=True)
class TemporalRuleConfig:
    reference_time: datetime | None = None
    recent_touch_days: int = 14
    stale_stage_days: int = 21


@dataclass(frozen=True)
class DemoConfig:
    before_bundle: str = "bad/demo-before-cleanup"
    after_bundle: str = "good"


@dataclass(frozen=True)
class BusinessRuleConfig:
    name: str
    rule: BusinessRuleCallable


@dataclass(frozen=True)
class PipelineConfig:
    data_root: Path = Path("data")
    bundle: str = "good"
    reports_dir: Path = Path("reports")
    sql_dir: Path = Path("sql")
    schema_targets: Tuple[SchemaTargetConfig, ...] = field(
        default_factory=lambda: tuple(
            SchemaTargetConfig(name=target) for target in SCHEMA_TARGETS
        )
    )
    temporal: TemporalRuleConfig = field(default_factory=TemporalRuleConfig)
    demo: DemoConfig = field(default_factory=DemoConfig)
    business_rules: Tuple[BusinessRuleConfig, ...] = field(default_factory=tuple)


__all__ = [
    "BusinessRuleCallable",
    "BusinessRuleConfig",
    "DemoConfig",
    "PipelineConfig",
    "SchemaTargetConfig",
    "TablesMapping",
    "TemporalRuleConfig",
]
