"""Pandas-based ingestion helpers used by the validation layer.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Mapping, Optional, Sequence

import pandas as pd

DEFAULT_READ_KWARGS: Dict[str, Any] = {
    "keep_default_na": True,
    "na_values": [""],
    "encoding": "utf-8",
}


@dataclass
class LoadedTable:
    """Captures a DataFrame together with ingestion metadata."""

    name: str
    df: pd.DataFrame
    path: Path
    modified_at: datetime
    metadata: Dict[str, Any] = field(default_factory=dict)


def discover_ingest_files(base_dir: Path, file_names: Sequence[str]) -> Dict[str, Path]:
    """Make sure every named feed file is present under ``base_dir``."""

    base_dir = Path(base_dir)
    missing: list[Path] = []
    resolved: Dict[str, Path] = {}
    for candidate in file_names:
        path = (base_dir / candidate).expanduser()
        if path.exists():
            resolved[candidate] = path
        else:
            missing.append(path)
    if missing:
        missing_list = ", ".join(str(entry) for entry in missing)
        raise FileNotFoundError(f"Missing ingest files: {missing_list}")
    return resolved


def load_table(
    name: str,
    path: Path,
    parse_dates: Optional[Sequence[str]] = None,
    dtype_overrides: Optional[Mapping[str, Any]] = None,
    metadata: Optional[Mapping[str, Any]] = None,
    **csv_kwargs: Any,
) -> LoadedTable:
    """Load a CSV into a DataFrame while capturing filesystem metadata."""

    resolved_path = Path(path).expanduser()
    if not resolved_path.exists():
        raise FileNotFoundError(f"Ingest file not found: {resolved_path}")

    read_kwargs = {**DEFAULT_READ_KWARGS, **csv_kwargs}
    df = pd.read_csv(
        resolved_path,
        parse_dates=list(parse_dates) if parse_dates else None,
        dtype=dict(dtype_overrides) if dtype_overrides else None,
        **read_kwargs,
    )

    stat = resolved_path.stat()
    modified_at = datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc)

    metadata_dict: Dict[str, Any] = {
        "source_path": str(resolved_path),
        "file_size": stat.st_size,
        "loaded_at": datetime.now(timezone.utc).isoformat(),
    }
    if metadata:
        metadata_dict.update({k: v for k, v in metadata.items() if v is not None})

    return LoadedTable(
        name=name,
        df=df,
        path=resolved_path,
        modified_at=modified_at,
        metadata=metadata_dict,
    )
