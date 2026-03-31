"""High-level ingestion helpers exposed to the CertGate library."""

from __future__ import annotations

from pathlib import Path
from typing import Dict, Sequence

from .loaders import (
    LoadedTable,
    attach_bundle_metadata,
    derive_bundle_reference_time,
    discover_ingest_files,
    load_table,
)
from ..rules.schema import SCHEMA_TARGETS, get_schema_definition

__all__ = [
    "LoadedTable",
    "attach_bundle_metadata",
    "derive_bundle_reference_time",
    "discover_ingest_files",
    "load_bundle",
    "load_table",
]


def load_bundle(
    data_root: Path | str,
    bundle: str = "good",
    targets: Sequence[str] | None = None,
) -> Dict[str, LoadedTable]:
    """Load every table in ``targets`` from the requested dataset bundle."""

    root = Path(data_root)
    bundle_path = (root / bundle).expanduser()
    schema_targets = targets or SCHEMA_TARGETS
    loaded: Dict[str, LoadedTable] = {}
    for name in schema_targets:
        definition = get_schema_definition(name)
        loaded[name] = load_table(
            name=name,
            path=bundle_path / definition.file_name,
            parse_dates=definition.parse_dates,
            dtype_overrides=definition.dtype_map,
            metadata={"timestamp_columns": definition.reference_timestamp_columns},
        )

    reference_time = derive_bundle_reference_time(loaded)
    attach_bundle_metadata(loaded, bundle_name=bundle, reference_time=reference_time)
    return loaded
