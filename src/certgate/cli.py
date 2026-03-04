"""Command-line orchestrator for the CertGate release-report workflow."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Sequence

from certgate.pipeline import ReleaseGatePipeline
from certgate.config import PipelineConfig


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="Run the CertGate pipeline and emit release reports."
    )
    parser.add_argument(
        "--data-root",
        type=Path,
        default=Path("data"),
        help="Root directory that hosts dataset bundles (default: data).",
    )
    parser.add_argument(
        "--bundle",
        default="good",
        help="Named bundle under data root to load (default: good).",
    )
    parser.add_argument(
        "--reports-dir",
        type=Path,
        default=Path("reports"),
        help="Directory to emit release report JSON (default: reports).",
    )
    args = parser.parse_args(argv)

    config = PipelineConfig(
        data_root=args.data_root,
        bundle=args.bundle,
        reports_dir=args.reports_dir,
    )
    pipeline = ReleaseGatePipeline(config)
    pipeline.run()
    pipeline.write_reports()
    print("Release reports written to", args.reports_dir.resolve())


__all__ = ["main"]
