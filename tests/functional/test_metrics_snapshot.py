from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parents[2]


def test_metrics_snapshot_generation(tmp_path: Path) -> None:
    reports_dir = tmp_path / "reports"

    subprocess.run(
        [
            sys.executable,
            "scripts/generate_release_reports.py",
            "--reports-dir",
            str(reports_dir),
        ],
        cwd=BASE_DIR,
        check=True,
    )
    subprocess.run(
        [
            sys.executable,
            "scripts/generate_metrics_snapshot.py",
            "--reports-dir",
            str(reports_dir),
        ],
        cwd=BASE_DIR,
        check=True,
    )

    json_path = reports_dir / "metrics_snapshot.json"
    md_path = reports_dir / "metrics_snapshot.md"

    assert json_path.exists()
    assert md_path.exists()

    payload = json.loads(json_path.read_text(encoding="utf-8"))
    assert payload["project"] == "CertGate"
    assert payload["data_quality"]["tables_validated"] == 5
    assert payload["data_quality"]["rule_count"] >= 90
    assert payload["data_quality"]["release_status"] in {"Ready", "Blocked"}
    assert (
        payload["demo_before_after"]["before_defects"]
        > payload["demo_before_after"]["after_defects"]
    )
    assert payload["great_expectations"]["suite_count"] >= 5
    assert payload["testing"]["regression_scenarios"] >= 9
