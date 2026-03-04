from pathlib import Path

import pytest

from certgate.rules.business import check_exam_candidate_fk, evaluate_table_freshness
from certgate.rules.schema import apply_schema_definition, get_schema_definition

REPO_ROOT = Path(__file__).resolve().parents[2]
DATA_ROOT = REPO_ROOT / "data"
REGRESSION_DIR = DATA_ROOT / "regression"

pytestmark = pytest.mark.regression


@pytest.mark.parametrize(
    "dataset_bundle,expected_prefix",
    [
        pytest.param(
            REGRESSION_DIR / "regression-duplicate-exam-attempt",
            "exam_results-uniqueness",
            id="duplicate-exam-attempt",
        ),
    ],
    indirect=["dataset_bundle"],
)
def test_regression_duplicate_attempt_triggered(dataset_bundle, expected_prefix):
    exam_table = dataset_bundle["exam_results"]
    _, outcomes = apply_schema_definition(
        exam_table.df,
        get_schema_definition("exam_results"),
    )
    duplicate_issue = next(
        (
            outcome
            for outcome in outcomes
            if outcome.rule_id.startswith(expected_prefix) and not outcome.passed
        ),
        None,
    )
    assert duplicate_issue is not None
    assert not duplicate_issue.passed
    assert duplicate_issue.severity == "critical"


@pytest.mark.parametrize(
    "dataset_bundle",
    [
        pytest.param(
            REGRESSION_DIR / "regression-lagging-exam-file",
            id="lagging-exam-file",
        ),
    ],
    indirect=["dataset_bundle"],
)
def test_regression_lagging_exam_files_block_release(dataset_bundle):
    exam_table = dataset_bundle["exam_results"]
    freshness_outcome = evaluate_table_freshness(
        exam_table,
        timestamp_column="file_received_ts",
        max_allowed_hours=24,
        warning_hours=48,
    )
    assert not freshness_outcome.passed
    assert freshness_outcome.severity == "critical"


@pytest.mark.parametrize(
    "dataset_bundle",
    [
        pytest.param(
            REGRESSION_DIR / "regression-missing-candidate-join",
            id="missing-candidate-join",
        ),
    ],
    indirect=["dataset_bundle"],
)
def test_regression_missing_candidate_fk(dataset_bundle):
    exam_table = dataset_bundle["exam_results"]
    candidates_table = dataset_bundle["candidates"]
    fk_outcome = check_exam_candidate_fk(exam_table.df, candidates_table.df)
    assert not fk_outcome.passed
    assert fk_outcome.severity == "critical"
