import json
from pathlib import Path

import pytest

from certgate.reporting import ReleaseReport, STATUS_BLOCKED, STATUS_WARNING_ONLY
from certgate.rules.business import evaluate_table_freshness
from certgate.rules.schema import RuleOutcome

REPO_ROOT = Path(__file__).resolve().parents[2]
DATA_ROOT = REPO_ROOT / "data"


pytestmark = pytest.mark.uat


@pytest.mark.parametrize(
    "dataset_bundle",
    [
        pytest.param(
            DATA_ROOT / "regression" / "regression-lagging-exam-file",
            id="lagging-exam-file",
        ),
    ],
    indirect=["dataset_bundle"],
)
def test_release_decision_blocks_lagging_bundle(dataset_bundle, tmp_path):
    exam_table = dataset_bundle["exam_results"]
    freshness_outcome = evaluate_table_freshness(
        exam_table,
        timestamp_column="file_received_ts",
        max_allowed_hours=24,
        warning_hours=48,
    )
    report = ReleaseReport([freshness_outcome])
    assert report.status == STATUS_BLOCKED
    reports_dir = tmp_path / "reports"
    report.write_reports(reports_dir)
    decision_payload = json.loads((reports_dir / "release_decision.json").read_text())
    assert decision_payload["status"] == STATUS_BLOCKED
    assert decision_payload["blocking_root_causes"] == ["stale_data"]


def test_release_decision_warns_when_only_warnings():
    warning_outcome = RuleOutcome(
        rule_id="BR-04",
        description="Score exceeds range",
        passed=False,
        severity="warning",
    )
    report = ReleaseReport([warning_outcome])
    assert report.status == STATUS_WARNING_ONLY
    decision_payload = report.release_decision()
    assert decision_payload["status"] == STATUS_WARNING_ONLY
    assert "blocking_root_causes" not in decision_payload
