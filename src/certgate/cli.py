"""Command-line orchestrator for the CertGate CRM integrity workflow."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Sequence

from certgate.config import PipelineConfig
from certgate.pipeline import ReleaseGatePipeline


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="Run the CertGate CRM integrity pipeline and emit reports."
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
        help="Directory to emit report artifacts (default: reports).",
    )
    parser.add_argument(
        "--sql-dir",
        type=Path,
        default=Path("sql"),
        help="Directory containing DuckDB SQL insights (default: sql).",
    )
    args = parser.parse_args(argv)

    config = PipelineConfig(
        data_root=args.data_root,
        bundle=args.bundle,
        reports_dir=args.reports_dir,
        sql_dir=args.sql_dir,
    )
    pipeline = ReleaseGatePipeline(config)
    report = pipeline.generate_all_outputs(bundle_name=args.bundle)
    print(
        "CertGate CRM integrity artifacts written to",
        args.reports_dir.resolve(),
        f"(status: {report.status})",
    )


__all__ = ["main"]
