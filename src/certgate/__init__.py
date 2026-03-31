"""Convenience entry point for the CertGate core API."""

from __future__ import annotations

from .analytics import OPS_QUERY_NAMES, build_weekly_exec_summary, run_ops_insights
from .ingest import LoadedTable, discover_ingest_files, load_bundle, load_table
from .pipeline import BundleRun, ReleaseGatePipeline
from .reporting import ReleaseReport, ReportWriter
from .rules.schema import RuleOutcome, SchemaDefinition
from .config import DemoConfig, PipelineConfig, SchemaTargetConfig, TemporalRuleConfig

__all__ = [
    "BundleRun",
    "DemoConfig",
    "LoadedTable",
    "OPS_QUERY_NAMES",
    "PipelineConfig",
    "ReleaseGatePipeline",
    "ReleaseReport",
    "ReportWriter",
    "RuleOutcome",
    "SchemaDefinition",
    "SchemaTargetConfig",
    "TemporalRuleConfig",
    "build_weekly_exec_summary",
    "discover_ingest_files",
    "load_bundle",
    "load_table",
    "run_ops_insights",
]
