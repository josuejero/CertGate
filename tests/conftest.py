"""Shared fixtures and helpers for the CertGate QA catalog."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Dict, Iterable

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from certgate.ingest.loaders import LoadedTable, load_table

DATA_ROOT = REPO_ROOT / "data"


def _load_csv_bundle(bundle_path: Path) -> Dict[str, LoadedTable]:
    resolved = Path(bundle_path).expanduser().resolve()
    if not resolved.exists():
        raise FileNotFoundError(f"Dataset bundle not found: {resolved}")
    tables: Dict[str, LoadedTable] = {}
    for csv_path in sorted(resolved.glob("*.csv")):
        tables[csv_path.stem] = load_table(name=csv_path.stem, path=csv_path)
    return tables


@pytest.fixture(scope="session")
def good_bundle() -> Dict[str, LoadedTable]:
    """Load the canonical good dataset bundle once per test session."""

    return _load_csv_bundle(DATA_ROOT / "good")


@pytest.fixture
def dataset_bundle(request) -> Dict[str, LoadedTable]:
    """Load a dataset bundle supplied via indirect parametrization."""

    bundle_path: Path = request.param
    return _load_csv_bundle(bundle_path)


def bundle_path(*parts: str) -> Path:
    """Return a resolved bundle path under the repository data directory."""

    return DATA_ROOT.joinpath(*parts).resolve()


def parametrized_bundles(kind: str) -> Iterable[Path]:
    """Return every bundle path for a named data directory."""

    parent = DATA_ROOT / kind
    return (entry.resolve() for entry in sorted(parent.iterdir()) if entry.is_dir())
