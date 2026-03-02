import pandas as pd

from src.rules.schema_rules import (
    check_required_columns,
    detect_duplicates,
    normalize_dataframe_dtypes,
)


def test_required_columns_failure_reports_missing_entries():
    df = pd.DataFrame({"candidate_id": ["1"], "email": ["foo@example.com"]})
    outcome = check_required_columns(
        df=df,
        required_columns=["candidate_id", "first_name", "last_name"],
        rule_id="schema-missing-columns",
    )
    assert not outcome.passed
    assert outcome.details["missing_columns"] == ["first_name", "last_name"]


def test_detect_duplicates_marks_repeated_rows():
    df = pd.DataFrame({"candidate_id": ["a", "a", "b"], "score": [90, 95, 88]})
    outcome = detect_duplicates(
        df=df,
        subset=["candidate_id"],
        rule_id="schema-duplicate-report",
    )
    assert not outcome.passed
    assert outcome.details["duplicate_count"] == 2
    assert outcome.details["subset"] == ["candidate_id"]


def test_normalize_dataframe_dtypes_emits_issues_for_bad_values():
    df = pd.DataFrame({"score": ["100", "bad"]})
    normalized, issues = normalize_dataframe_dtypes(
        df=df,
        dtype_map={"score": "float64"},
        rule_id_prefix="schema-dtype",
    )
    assert len(issues) == 1
    issue = issues[0]
    assert issue.rule_id == "schema-dtype-score"
    assert not issue.passed
    assert "Could not coerce" in issue.description
    assert normalized["score"].dtype == object
