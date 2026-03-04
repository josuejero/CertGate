import pytest
from pathlib import Path

from certgate.pipeline import PipelineConfig, ReleaseGatePipeline
from certgate.reporting import STATUS_BLOCKED
from certgate.rules.business import check_exam_candidate_fk, check_pass_fail_certification
from certgate.rules.schema import apply_schema_definition, get_schema_definition


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


def test_pipeline_status_for_good_bundle(tmp_path):
    repo_root = Path(__file__).resolve().parents[2]
    config = PipelineConfig(
        data_root=repo_root / "data",
        bundle="good",
        reports_dir=tmp_path / "reports",
    )
    pipeline = ReleaseGatePipeline(config)
    report = pipeline.run()
    assert report.status == STATUS_BLOCKED
    pipeline.write_reports(report_dir=tmp_path / "reports")
