import pytest

from src.rules.business_rules import check_exam_candidate_fk, check_pass_fail_certification
from src.rules.schema_rules import apply_schema_definition, get_schema_definition


SCHEMA_TARGETS = ("candidates", "exam_results", "certification_status")

pytestmark = pytest.mark.functional


@pytest.mark.parametrize("schema_name", SCHEMA_TARGETS)
def test_schema_validation_passes_for_canonical_data(schema_name, good_bundle):
    table = good_bundle[schema_name]
    _, outcomes = apply_schema_definition(table.df, get_schema_definition(schema_name))
    required_rule = next(
        outcome for outcome in outcomes if outcome.rule_id == f"{schema_name}-required-columns"
    )
    assert required_rule.passed
    assert not [
        outcome for outcome in outcomes if outcome.severity == "critical" and not outcome.passed
    ]


def test_pass_fail_and_candidate_fk_criteria_hold(good_bundle):
    exam_df = good_bundle["exam_results"].df
    candidates_df = good_bundle["candidates"].df
    certification_df = good_bundle["certification_status"].df

    fk_outcome = check_exam_candidate_fk(exam_df, candidates_df)
    assert fk_outcome.passed

    business_outcomes = check_pass_fail_certification(exam_df, certification_df)
    assert all(outcome.passed for outcome in business_outcomes)
