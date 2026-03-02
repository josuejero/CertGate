from pathlib import Path

import pytest

from src.ingest.loaders import discover_ingest_files

REPO_ROOT = Path(__file__).resolve().parents[2]
DATA_ROOT = REPO_ROOT / "data"
EXPECTATIONS_DIR = REPO_ROOT / "gx" / "expectations"
CHECKPOINT_PATH = REPO_ROOT / "gx" / "checkpoints" / "ingest_validation_checkpoint.yml"

pytestmark = pytest.mark.smoke


def test_schema_and_rules_import_cleanly():
    import src.rules.schema_rules as schema_rules
    import src.rules.business_rules as business_rules

    # Spot-check that the modules expose the expected helpers.
    assert schema_rules.SCHEMA_DEFINITIONS
    assert hasattr(business_rules, "PASSING_VALUES")


def test_good_files_are_discoverable():
    base = DATA_ROOT / "good"
    discovered = discover_ingest_files(
        base_dir=base,
        file_names=["candidates.csv", "exam_results.csv", "certification_status.csv"],
    )
    assert set(discovered.keys()) == {
        "candidates.csv",
        "exam_results.csv",
        "certification_status.csv",
    }


def test_checkpoint_references_expectation_suites():
    suites = []
    with CHECKPOINT_PATH.open() as checkpoint_file:
        for line in checkpoint_file:
            if line.strip().startswith("expectation_suite_name:"):
                _, suite = line.split(":", 1)
                suites.append(suite.strip())
    assert suites, "checkpoint must declare at least one expectation suite"

    for suite_name in suites:
        expectation_path = EXPECTATIONS_DIR / f"{suite_name}.yml"
        assert expectation_path.exists(), f"{suite_name} expectation suite missing"
