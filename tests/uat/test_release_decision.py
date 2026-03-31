import json
from pathlib import Path

import pytest

from certgate.config import PipelineConfig
from certgate.pipeline import ReleaseGatePipeline
from certgate.reporting import STATUS_BLOCKED, STATUS_READY

REPO_ROOT = Path(__file__).resolve().parents[2]

pytestmark = pytest.mark.uat


def test_demo_artifacts_show_before_blocked_and_after_ready(tmp_path: Path):
    pipeline = ReleaseGatePipeline(
        PipelineConfig(
            data_root=REPO_ROOT / "data",
            reports_dir=tmp_path,
            sql_dir=REPO_ROOT / "sql",
        )
    )
    pipeline.run(bundle_name="good")
    pipeline.write_demo_artifacts(report_dir=tmp_path)

    before = json.loads((tmp_path / "demo" / "before" / "release_decision.json").read_text())
    after = json.loads((tmp_path / "demo" / "after" / "release_decision.json").read_text())

    assert before["status"] == STATUS_BLOCKED
    assert after["status"] == STATUS_READY
