from pathlib import Path

import pytest

from certgate.config import PipelineConfig
from certgate.pipeline import ReleaseGatePipeline
from certgate.reporting import STATUS_BLOCKED, STATUS_WARNING_ONLY

REPO_ROOT = Path(__file__).resolve().parents[2]
DATA_ROOT = REPO_ROOT / "data"
REGRESSION_DIR = DATA_ROOT / "regression"

pytestmark = pytest.mark.regression


@pytest.mark.parametrize(
    "bundle_name,expected_status,expected_rules",
    [
        ("duplicate-lead-email", STATUS_BLOCKED, {"leads-uniqueness-unique-2"}),
        ("duplicate-account-domain", STATUS_BLOCKED, {"accounts-uniqueness-unique-2"}),
        ("email-multiple-accounts", STATUS_BLOCKED, {"crm-email-multiple-accounts"}),
        (
            "missing-owner-reference",
            STATUS_BLOCKED,
            {
                "crm-lead-owner-fk",
                "crm-opportunity-account-fk",
                "crm-activity-object-reference",
            },
        ),
        (
            "inactive-owner-open-opportunity",
            STATUS_BLOCKED,
            {"crm-inactive-owner-open-opportunity"},
        ),
        (
            "missing-employee-count-enterprise",
            STATUS_WARNING_ONLY,
            {"crm-enterprise-employee-count"},
        ),
        ("missing-next-step", STATUS_WARNING_ONLY, {"crm-open-opportunity-next-step"}),
        ("stale-opportunity-stage", STATUS_WARNING_ONLY, {"crm-stale-opportunity-stage"}),
        (
            "lead-account-domain-mismatch",
            STATUS_WARNING_ONLY,
            {"crm-lead-account-domain-mismatch"},
        ),
    ],
)
def test_regression_bundle_triggers_expected_rules(bundle_name, expected_status, expected_rules):
    pipeline = ReleaseGatePipeline(
        PipelineConfig(
            data_root=DATA_ROOT,
            bundle=f"regression/{bundle_name}",
            reports_dir=REPO_ROOT / "reports",
            sql_dir=REPO_ROOT / "sql",
        )
    )
    report = pipeline.run(bundle_name=f"regression/{bundle_name}")
    assert report.status == expected_status
    failing_ids = {outcome.rule_id for outcome in report.failing_outcomes}
    assert expected_rules.issubset(failing_ids)
