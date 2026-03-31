from pathlib import Path

import pytest

from certgate.ingest import discover_ingest_files

REPO_ROOT = Path(__file__).resolve().parents[2]
DATA_ROOT = REPO_ROOT / "data"
EXPECTATIONS_DIR = REPO_ROOT / "gx" / "expectations"
CHECKPOINT_PATH = REPO_ROOT / "gx" / "checkpoints" / "ingest_validation_checkpoint.yml"

pytestmark = pytest.mark.smoke


def test_schema_and_rules_import_cleanly():
    import certgate.rules.schema as schema_rules
    import certgate.rules.business as business_rules

    assert set(schema_rules.SCHEMA_DEFINITIONS) == {
        "leads",
        "accounts",
        "opportunities",
        "activities",
        "owners",
    }
    assert business_rules.LEAD_LIFECYCLE_ORDER[-1] == "Customer"


def test_good_files_are_discoverable():
    base = DATA_ROOT / "good"
    discovered = discover_ingest_files(
        base_dir=base,
        file_names=[
            "leads.csv",
            "accounts.csv",
            "opportunities.csv",
            "activities.csv",
            "owners.csv",
        ],
    )
    assert set(discovered.keys()) == {
        "leads.csv",
        "accounts.csv",
        "opportunities.csv",
        "activities.csv",
        "owners.csv",
    }


def test_checkpoint_references_crm_expectation_suites():
    checkpoint_text = CHECKPOINT_PATH.read_text()
    expected_suites = (
        "leads_suite",
        "accounts_suite",
        "opportunities_suite",
        "activities_suite",
        "owners_suite",
        "freshness_suite",
    )
    for suite_name in expected_suites:
        assert suite_name in checkpoint_text
        assert (EXPECTATIONS_DIR / f"{suite_name}.json").exists()
