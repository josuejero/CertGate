from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from certgate.ingest.loaders import LoadedTable
from certgate.rules.business import (
    check_pass_fail_certification,
    check_score_range,
    check_exam_date_window,
    evaluate_table_freshness,
)


def test_score_range_warns_for_invalid_entries():
    df = pd.DataFrame(
        {
            "exam_result_id": ["ER-1", "ER-2"],
            "score": [95, 120],
            "score_scaled": [95, 120],
        }
    )
    outcome = check_score_range(
        exam_df=df,
        column="score",
        rule_id="BR-04",
        max_value=100,
    )
    assert not outcome.passed
    assert outcome.severity == "warning"
    assert outcome.details["out_of_range_count"] == 1


def test_exam_date_window_flags_future_dates():
    reference = datetime(2026, 3, 1, tzinfo=timezone.utc)
    df = pd.DataFrame(
        {"exam_result_id": ["ER-1"], "exam_date": ["2027-04-01T00:00:00Z"]}
    )
    outcome = check_exam_date_window(
        exam_df=df,
        reference_date=reference,
        months=12,
        rule_id="BR-06",
    )
    assert not outcome.passed
    assert outcome.severity == "warning"
    assert outcome.details["out_of_window"] == 1


def test_pass_fail_certification_detects_mismatch():
    exam_df = pd.DataFrame(
        {
            "candidate_id": ["C-1"],
            "exam_result_id": ["ER-1"],
            "pass_fail": ["Pass"],
        }
    )
    cert_df = pd.DataFrame(
        {
            "candidate_id": ["C-1"],
            "certification_status": ["NeedsReview"],
            "status_effective_ts": ["2026-02-01T00:00:00Z"],
        }
    )
    outcomes = check_pass_fail_certification(exam_df=exam_df, cert_df=cert_df)
    outcome = next(o for o in outcomes if o.rule_id.endswith("-pass"))
    assert not outcome.passed
    assert outcome.severity == "critical"


def test_evaluate_table_freshness_uses_file_timestamp():
    df = pd.DataFrame(
        {"file_received_ts": ["2026-01-10T08:00:00Z"], "exam_result_id": ["ER-100"]}
    )
    metadata = {"source_path": "tests/unit/test_stub"}
    table = LoadedTable(
        name="exam_results",
        df=df,
        path=Path("data/regression/regression-lagging-exam-file/exam_results.csv"),
        modified_at=datetime(2026, 1, 10, tzinfo=timezone.utc),
        metadata=metadata,
    )
    outcome = evaluate_table_freshness(
        table=table,
        timestamp_column="file_received_ts",
        max_allowed_hours=24,
        warning_hours=48,
    )
    assert not outcome.passed
    assert outcome.severity == "critical"
