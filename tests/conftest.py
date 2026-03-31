"""Shared fixtures and helpers for the CertGate CRM catalog."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Iterable

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from certgate.ingest import load_bundle

DATA_ROOT = REPO_ROOT / "data"


@pytest.fixture(scope="session")
def good_bundle():
    """Load the canonical CRM bundle once per test session."""

    return load_bundle(DATA_ROOT, "good")


@pytest.fixture
def dataset_bundle(request):
    """Load a dataset bundle supplied via indirect parametrization."""

    bundle_path = Path(request.param).resolve()
    relative = bundle_path.relative_to(DATA_ROOT)
    return load_bundle(DATA_ROOT, str(relative))


def bundle_path(*parts: str) -> Path:
    """Return a resolved bundle path under the repository data directory."""

    return DATA_ROOT.joinpath(*parts).resolve()


def parametrized_bundles(kind: str) -> Iterable[Path]:
    """Return every bundle path for a named data directory."""

    parent = DATA_ROOT / kind
    return (entry.resolve() for entry in sorted(parent.iterdir()) if entry.is_dir())
