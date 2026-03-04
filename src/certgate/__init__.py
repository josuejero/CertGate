"""Convenience entry point for the CertGate core API."""

from __future__ import annotations

from .ingest import LoadedTable, discover_ingest_files, load_bundle, load_table
from .pipeline import PipelineConfig, ReleaseGatePipeline
from .reporting import ReleaseReport, ReportWriter
from .rules.schema import RuleOutcome, SchemaDefinition

__all__ = [
    "LoadedTable",
    "load_table",
    "load_bundle",
    "discover_ingest_files",
    "RuleOutcome",
    "SchemaDefinition",
    "ReleaseReport",
    "ReportWriter",
    "PipelineConfig",
    "ReleaseGatePipeline",
]
