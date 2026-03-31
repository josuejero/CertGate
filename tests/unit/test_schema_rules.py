import pandas as pd

from certgate.rules.schema import (
    apply_schema_definition,
    check_required_columns,
    detect_duplicates,
    get_schema_definition,
    normalize_dataframe_dtypes,
)


def test_required_columns_failure_reports_missing_entries():
    df = pd.DataFrame({"lead_id": ["1"], "email": ["foo@example.com"]})
    outcome = check_required_columns(
        df=df,
        required_columns=["lead_id", "email", "company_domain"],
        rule_id="schema-missing-columns",
    )
    assert not outcome.passed
    assert outcome.details["missing_columns"] == ["company_domain"]


def test_detect_duplicates_marks_repeated_rows():
    df = pd.DataFrame({"company_domain": ["a.com", "a.com", "b.com"]})
    outcome = detect_duplicates(
        df=df,
        subset=["company_domain"],
        rule_id="schema-duplicate-report",
    )
    assert not outcome.passed
    assert outcome.details["duplicate_count"] == 2
    assert outcome.details["subset"] == ["company_domain"]


def test_normalize_dataframe_dtypes_emits_issues_for_bad_values():
    df = pd.DataFrame(
        {
            "created_at": ["2026-03-01T00:00:00Z", "not-a-date"],
            "is_active": ["true", "maybe"],
        }
    )
    normalized, issues = normalize_dataframe_dtypes(
        df=df,
        dtype_map={"created_at": "datetime64[ns, UTC]", "is_active": "boolean"},
        rule_id_prefix="schema-dtype",
    )
    failing = [issue for issue in issues if not issue.passed]
    assert len(failing) == 2
    assert normalized["created_at"].isna().sum() == 1
    assert normalized["is_active"].isna().sum() == 1


def test_apply_schema_definition_catches_duplicate_lead_email():
    definition = get_schema_definition("leads")
    df = pd.DataFrame(
        {
            "lead_id": ["L-1", "L-2"],
            "email": ["dup@example.com", "dup@example.com"],
            "first_name": ["A", "B"],
            "last_name": ["One", "Two"],
            "company_name": ["Acme", "Acme"],
            "company_domain": ["acme.com", "acme.com"],
            "job_title": ["Ops", "Ops"],
            "owner_id": ["O-1", "O-1"],
            "lead_source": ["Inbound", "Inbound"],
            "lifecycle_stage": ["Lead", "Lead"],
            "lead_status": ["New", "New"],
            "created_at": ["2026-03-01T00:00:00Z", "2026-03-02T00:00:00Z"],
            "last_activity_at": ["2026-03-01T00:00:00Z", "2026-03-02T00:00:00Z"],
            "account_id": ["A-1", "A-1"],
        }
    )
    _, outcomes = apply_schema_definition(df, definition)
    duplicate_rule = next(
        outcome for outcome in outcomes if outcome.rule_id == "leads-uniqueness-unique-2"
    )
    assert not duplicate_rule.passed
