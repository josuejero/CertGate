"""Pandas-based business logic validations for Tier A."""

from __future__ import annotations

from datetime import datetime, timezone, timedelta
from typing import Any, Iterable, List, Sequence

import pandas as pd

from ..ingest.loaders import LoadedTable
from .schema_rules import RuleOutcome, Severity, build_rule_outcome

PASSING_VALUES = {"Pass", "pass", "P"}
FAILING_VALUES = {"Fail", "fail", "F"}
CERTIFIED_STATUSES = {"Certified", "Certification Granted"}
FAIL_ALLOWED_STATUSES = {"NeedsReview", "Certification Pending", "Not Certified"}


def _foreign_key_reference(
    child_df: pd.DataFrame,
    parent_df: pd.DataFrame,
    child_column: str,
    parent_column: str,
    rule_id: str,
    description: str,
    severity: Severity = "critical",
) -> RuleOutcome:
    source = child_df[child_column].dropna().unique()
    parent_values = set(parent_df[parent_column].dropna())
    missing = sorted({value for value in source if value not in parent_values})
    passed = not missing
    details: dict[str, Any] = {"missing_count": len(missing), "missing_values": missing}
    return build_rule_outcome(
        rule_id=rule_id,
        description=description if passed else description + " Mismatched candidates found.",
        passed=passed,
        severity=severity if not passed else "info",
        details=details,
    )


def check_exam_candidate_fk(
    exam_df: pd.DataFrame, candidates_df: pd.DataFrame, rule_id: str = "BR-07"
) -> RuleOutcome:
    """Ensure every exam result refers back to the candidate master."""

    return _foreign_key_reference(
        child_df=exam_df,
        parent_df=candidates_df,
        child_column="candidate_id",
        parent_column="candidate_id",
        rule_id=rule_id,
        description="Every exam candidate_id exists in the master list.",
    )


def check_cert_candidate_fk(
    cert_df: pd.DataFrame, candidates_df: pd.DataFrame, rule_id: str = "BR-07b"
) -> RuleOutcome:
    """Ensure every certification status row references an existing candidate."""

    return _foreign_key_reference(
        child_df=cert_df,
        parent_df=candidates_df,
        child_column="candidate_id",
        parent_column="candidate_id",
        rule_id=rule_id,
        description="Every certification row references a known candidate.",
    )


def _coerce_timestamp(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        ts = value
    else:
        ts = pd.to_datetime(value, utc=True, errors="coerce")
        if pd.isna(ts):
            return None
        ts = ts.to_pydatetime()
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return ts


def evaluate_table_freshness(
    table: LoadedTable,
    timestamp_column: str | None = None,
    metadata_timestamp_keys: Sequence[str] | None = None,
    max_allowed_hours: int = 24,
    warning_hours: int | None = None,
    rule_id: str = "BR-08",
) -> RuleOutcome:
    metadata_keys = metadata_timestamp_keys or ("file_received_ts", "ingest_time", "status_effective_ts")
    reported_ts: list[datetime] = []

    if timestamp_column and timestamp_column in table.df.columns:
        column_ts = pd.to_datetime(table.df[timestamp_column], utc=True, errors="coerce")
        if not column_ts.dropna().empty:
            reported_ts.append(column_ts.max().to_pydatetime())

    for key in metadata_keys:
        candidate = _coerce_timestamp(table.metadata.get(key))
        if candidate:
            reported_ts.append(candidate)

    if not reported_ts:
        data_ts = table.modified_at
        source_label = "filesystem"
    else:
        data_ts = max(reported_ts)
        source_label = timestamp_column or "metadata"

    reference_ts = _coerce_timestamp(table.metadata.get("ingest_time")) or datetime.now(timezone.utc)
    warning_window = warning_hours if warning_hours is not None else max_allowed_hours * 2
    lag_hours = max(0.0, (reference_ts - data_ts).total_seconds() / 3600)

    if lag_hours <= max_allowed_hours:
        passed = True
        severity = "info"
        description = "Freshness is within allowed lag."
    elif lag_hours <= warning_window:
        passed = False
        severity = "warning"
        description = "Freshness crosses warning threshold."
    else:
        passed = False
        severity = "critical"
        description = "Freshness exceeds the critical lag threshold."

    details = {
        "lag_hours": round(lag_hours, 2),
        "reference_time": reference_ts.isoformat(),
        "latest_data_time": data_ts.isoformat(),
        "timestamp_source": source_label,
        "max_allowed_hours": max_allowed_hours,
        "warning_hours": warning_window,
    }
    return build_rule_outcome(
        rule_id=rule_id,
        description=description,
        passed=passed,
        severity=severity,
        details=details,
    )


def _summarize_candidates(values: Iterable[Any], limit: int = 5) -> list[Any]:
    return list(dict.fromkeys(values))[:limit]


def check_pass_fail_certification(
    exam_df: pd.DataFrame, cert_df: pd.DataFrame, rule_id_prefix: str = "BR-05"
) -> List[RuleOutcome]:
    latest_status = (
        cert_df.sort_values("status_effective_ts", na_position="last")
        .drop_duplicates("candidate_id", keep="last")
        .set_index("candidate_id")
    )
    merged = exam_df.join(latest_status[["certification_status"]], on="candidate_id", how="left")

    cert_status = merged["certification_status"].fillna("")
    pass_mask = merged["pass_fail"].isin(PASSING_VALUES)
    fail_mask = merged["pass_fail"].isin(FAILING_VALUES)

    pass_mismatch = pass_mask & ~cert_status.isin(CERTIFIED_STATUSES)
    pass_missing = pass_mask & merged["certification_status"].isna()
    fail_mismatch = fail_mask & cert_status.isin(CERTIFIED_STATUSES)

    outcomes: List[RuleOutcome] = []
    pass_failure = pass_mismatch.any() or pass_missing.any()
    pass_issue = build_rule_outcome(
        rule_id=f"{rule_id_prefix}-pass",
        description="Pass exams should map to Certified statuses.",
        passed=not pass_failure,
        severity="critical" if pass_failure else "info",
        details={
            "pass_mismatch_count": int(pass_mismatch.sum()),
            "pass_missing_count": int(pass_missing.sum()),
            "sample_candidates": _summarize_candidates(merged.loc[pass_mismatch | pass_missing, "candidate_id"].tolist()),
        },
    )
    outcomes.append(pass_issue)

    fail_failure = fail_mismatch.any()
    fail_issue = build_rule_outcome(
        rule_id=f"{rule_id_prefix}-fail",
        description="Fail attempts should not result in Certified statuses.",
        passed=not fail_failure,
        severity="warning" if fail_failure else "info",
        details={
            "fail_mismatch_count": int(fail_mismatch.sum()),
            "sample_candidates": _summarize_candidates(merged.loc[fail_mismatch, "candidate_id"].tolist()),
            "allowed_fail_statuses": sorted(FAIL_ALLOWED_STATUSES),
        },
    )
    outcomes.append(fail_issue)
    return outcomes


def check_score_range(
    exam_df: pd.DataFrame, column: str, rule_id: str, min_value: float = 0.0, max_value: float = 100.0
) -> RuleOutcome:
    values = pd.to_numeric(exam_df[column], errors="coerce")
    invalid = exam_df[column].notna() & values.isna()
    out_of_bounds = (values < min_value) | (values > max_value)
    invalid_count = int(invalid.sum())
    out_of_range_count = int(out_of_bounds.sum())
    passed = not invalid_count and not out_of_range_count
    description = (
        f"Column '{column}' stays within {min_value}-{max_value}." if passed else "Score range violation."
    )
    details = {
        "invalid_format_count": invalid_count,
        "out_of_range_count": out_of_range_count,
        "column": column,
    }
    return build_rule_outcome(
        rule_id=rule_id,
        description=description,
        passed=passed,
        severity="warning" if not passed else "info",
        details=details,
    )


def check_exam_date_window(
    exam_df: pd.DataFrame,
    reference_date: datetime | None = None,
    months: int = 12,
    rule_id: str = "BR-06",
) -> RuleOutcome:
    reference_date = reference_date or datetime.now(timezone.utc)
    window_days = int(30.5 * months)
    window_start = reference_date - timedelta(days=window_days)
    exam_dates = pd.to_datetime(exam_df["exam_date"], errors="coerce")
    out_of_window = (exam_dates < window_start) | (exam_dates > reference_date)
    count = int(out_of_window.sum())
    passed = count == 0
    description = (
        "Exam dates fall within the rolling 12-month window." if passed else "Exam dates are outside the allowed window."
    )
    details = {
        "window_start": window_start.isoformat(),
        "window_end": reference_date.isoformat(),
        "window_days": window_days,
        "out_of_window": count,
    }
    return build_rule_outcome(
        rule_id=rule_id,
        description=description,
        passed=passed,
        severity="warning" if not passed else "info",
        details=details,
    )
