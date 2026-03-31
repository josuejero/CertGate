from pathlib import Path

import pytest

from certgate.config import PipelineConfig
from certgate.pipeline import ReleaseGatePipeline

REPO_ROOT = Path(__file__).resolve().parents[2]

pytestmark = pytest.mark.defect_triage


def test_demo_bundle_has_expected_root_cause_mix():
    pipeline = ReleaseGatePipeline(
        PipelineConfig(
            data_root=REPO_ROOT / "data",
            bundle="bad/demo-before-cleanup",
            reports_dir=REPO_ROOT / "reports",
            sql_dir=REPO_ROOT / "sql",
        )
    )
    report = pipeline.run(bundle_name="bad/demo-before-cleanup")
    summary = report.defect_summary()
    assert summary["root_cause_counts"]["duplicate_record"] >= 1
    assert summary["root_cause_counts"]["integrity_issue"] >= 1
    assert summary["root_cause_counts"]["invalid_business_rule_state"] >= 1
