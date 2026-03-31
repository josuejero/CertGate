"""Schema-centric validators that exercise pandas before GX expectations fire."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Mapping, Sequence, Tuple

import pandas as pd

try:
    from typing import Literal
except ImportError:  # pragma: no cover - py39 compatibility
    from typing_extensions import Literal

Severity = Literal["critical", "warning", "info"]


@dataclass
class RuleOutcome:
    """Captures the results produced by pandas rule helpers."""

    rule_id: str
    description: str
    passed: bool
    severity: Severity
    details: Dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class SchemaDefinition:
    name: str
    file_name: str
    required_columns: Tuple[str, ...]
    non_null_columns: Tuple[str, ...]
    dtype_map: Mapping[str, Any]
    parse_dates: Tuple[str, ...]
    reference_timestamp_columns: Tuple[str, ...] = field(default_factory=tuple)
    uniqueness_constraints: Tuple[Tuple[str, ...], ...] = field(default_factory=tuple)


SCHEMA_DEFINITIONS: Dict[str, SchemaDefinition] = {
    "leads": SchemaDefinition(
        name="leads",
        file_name="leads.csv",
        required_columns=(
            "lead_id",
            "email",
            "first_name",
            "last_name",
            "company_name",
            "company_domain",
            "job_title",
            "owner_id",
            "lead_source",
            "lifecycle_stage",
            "lead_status",
            "created_at",
            "last_activity_at",
            "account_id",
        ),
        non_null_columns=("lead_id", "email"),
        parse_dates=("created_at", "last_activity_at"),
        reference_timestamp_columns=("created_at", "last_activity_at"),
        dtype_map={
            "lead_id": "string",
            "email": "string",
            "first_name": "string",
            "last_name": "string",
            "company_name": "string",
            "company_domain": "string",
            "job_title": "string",
            "owner_id": "string",
            "lead_source": "string",
            "lifecycle_stage": "string",
            "lead_status": "string",
            "created_at": "datetime64[ns, UTC]",
            "last_activity_at": "datetime64[ns, UTC]",
            "account_id": "string",
        },
        uniqueness_constraints=(("lead_id",), ("email",)),
    ),
    "accounts": SchemaDefinition(
        name="accounts",
        file_name="accounts.csv",
        required_columns=(
            "account_id",
            "account_name",
            "company_domain",
            "employee_count",
            "industry",
            "segment",
            "owner_id",
            "created_at",
            "last_activity_at",
        ),
        non_null_columns=("account_id", "company_domain"),
        parse_dates=("created_at", "last_activity_at"),
        reference_timestamp_columns=("created_at", "last_activity_at"),
        dtype_map={
            "account_id": "string",
            "account_name": "string",
            "company_domain": "string",
            "employee_count": "Int64",
            "industry": "string",
            "segment": "string",
            "owner_id": "string",
            "created_at": "datetime64[ns, UTC]",
            "last_activity_at": "datetime64[ns, UTC]",
        },
        uniqueness_constraints=(("account_id",), ("company_domain",)),
    ),
    "opportunities": SchemaDefinition(
        name="opportunities",
        file_name="opportunities.csv",
        required_columns=(
            "opportunity_id",
            "account_id",
            "owner_id",
            "stage",
            "amount",
            "created_at",
            "close_date",
            "last_stage_change_at",
            "next_step",
            "forecast_category",
        ),
        non_null_columns=("opportunity_id", "account_id", "owner_id", "stage"),
        parse_dates=("created_at", "close_date", "last_stage_change_at"),
        reference_timestamp_columns=("created_at", "last_stage_change_at"),
        dtype_map={
            "opportunity_id": "string",
            "account_id": "string",
            "owner_id": "string",
            "stage": "string",
            "amount": "float64",
            "created_at": "datetime64[ns, UTC]",
            "close_date": "datetime64[ns, UTC]",
            "last_stage_change_at": "datetime64[ns, UTC]",
            "next_step": "string",
            "forecast_category": "string",
        },
        uniqueness_constraints=(("opportunity_id",),),
    ),
    "activities": SchemaDefinition(
        name="activities",
        file_name="activities.csv",
        required_columns=(
            "activity_id",
            "object_type",
            "object_id",
            "activity_type",
            "activity_at",
            "owner_id",
            "outcome",
        ),
        non_null_columns=("activity_id", "object_type", "object_id", "owner_id"),
        parse_dates=("activity_at",),
        reference_timestamp_columns=("activity_at",),
        dtype_map={
            "activity_id": "string",
            "object_type": "string",
            "object_id": "string",
            "activity_type": "string",
            "activity_at": "datetime64[ns, UTC]",
            "owner_id": "string",
            "outcome": "string",
        },
        uniqueness_constraints=(("activity_id",),),
    ),
    "owners": SchemaDefinition(
        name="owners",
        file_name="owners.csv",
        required_columns=(
            "owner_id",
            "owner_name",
            "owner_email",
            "team",
            "manager",
            "is_active",
        ),
        non_null_columns=("owner_id", "owner_email"),
        parse_dates=(),
        reference_timestamp_columns=(),
        dtype_map={
            "owner_id": "string",
            "owner_name": "string",
            "owner_email": "string",
            "team": "string",
            "manager": "string",
            "is_active": "boolean",
        },
        uniqueness_constraints=(("owner_id",), ("owner_email",)),
    ),
}

SCHEMA_TARGETS: Tuple[str, ...] = tuple(SCHEMA_DEFINITIONS.keys())


def _json_safe(value: Any) -> Any:
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, (pd.Int64Dtype, pd.BooleanDtype)):
        return str(value)
    if pd.isna(value) if not isinstance(value, (list, dict, tuple, set)) else False:
        return None
    if hasattr(value, "item"):
        try:
            return value.item()
        except (TypeError, ValueError):
            return str(value)
    return value


def build_rule_outcome(
    rule_id: str,
    description: str,
    passed: bool,
    severity: Severity,
    details: Mapping[str, Any] | None = None,
) -> RuleOutcome:
    return RuleOutcome(
        rule_id=rule_id,
        description=description,
        passed=passed,
        severity=severity,
        details={key: _json_safe(value) if not isinstance(value, (list, dict)) else value for key, value in dict(details or {}).items()},
    )


def get_schema_definition(name: str) -> SchemaDefinition:
    """Return the schema definition for a named feed."""

    try:
        return SCHEMA_DEFINITIONS[name]
    except KeyError as exc:
        raise KeyError(f"No schema definition for '{name}'") from exc


def check_required_columns(
    df: pd.DataFrame,
    required_columns: Sequence[str],
    rule_id: str,
    severity: Severity = "critical",
) -> RuleOutcome:
    missing = [column for column in required_columns if column not in df.columns]
    passed = not missing
    description = (
        "All required columns are present." if passed else "Missing required columns."
    )
    details = {"missing_columns": missing, "affected_count": len(missing)} if missing else {"affected_count": 0}
    return build_rule_outcome(
        rule_id=rule_id,
        description=description,
        passed=passed,
        severity=severity if not passed else "info",
        details=details,
    )


def _blank_mask(series: pd.Series) -> pd.Series:
    if series.dtype.kind not in {"O", "U", "S"} and not pd.api.types.is_string_dtype(series):
        return pd.Series(False, index=series.index)
    normalized = series.fillna("").astype("string").str.strip()
    return normalized.eq("")


def check_non_null_columns(
    df: pd.DataFrame,
    columns: Sequence[str],
    rule_id_prefix: str,
    severity: Severity = "critical",
) -> List[RuleOutcome]:
    outcomes: List[RuleOutcome] = []
    for column in columns:
        if column not in df.columns:
            continue
        null_mask = df[column].isna() | _blank_mask(df[column])
        null_count = int(null_mask.sum())
        passed = null_count == 0
        description = (
            f"Column '{column}' contains no null primary-key values."
            if passed
            else f"Column '{column}' contains null or blank values."
        )
        details = {
            "column": column,
            "null_count": null_count,
            "affected_count": null_count,
        }
        outcomes.append(
            build_rule_outcome(
                rule_id=f"{rule_id_prefix}-{column}",
                description=description,
                passed=passed,
                severity=severity if not passed else "info",
                details=details,
            )
        )
    return outcomes


def _coerce_boolean_series(series: pd.Series) -> tuple[pd.Series, pd.Series]:
    normalized = series.fillna("").astype("string").str.strip().str.lower()
    mapping = {
        "true": True,
        "false": False,
        "1": True,
        "0": False,
        "yes": True,
        "no": False,
        "y": True,
        "n": False,
        "": pd.NA,
    }
    invalid_mask = ~normalized.isin(mapping)
    coerced = normalized.map(mapping).astype("boolean")
    return coerced, invalid_mask


def _coerce_series(series: pd.Series, target_dtype: Any) -> tuple[pd.Series, pd.Series]:
    target_label = str(target_dtype).lower()
    if "datetime" in target_label:
        converted = pd.to_datetime(series, errors="coerce", utc=True)
        invalid_mask = series.notna() & converted.isna()
        return converted, invalid_mask
    if target_label in {"float64", "float32", "float"}:
        converted = pd.to_numeric(series, errors="coerce")
        invalid_mask = series.notna() & converted.isna()
        return converted, invalid_mask
    if target_dtype == "Int64" or target_label in {"int64", "int32", "int"}:
        numeric = pd.to_numeric(series, errors="coerce")
        fractional_mask = numeric.notna() & (numeric % 1 != 0)
        invalid_mask = series.notna() & (numeric.isna() | fractional_mask)
        safe_numeric = numeric.mask(fractional_mask)
        return safe_numeric.astype("Int64"), invalid_mask
    if target_label in {"boolean", "bool"}:
        converted, invalid_mask = _coerce_boolean_series(series)
        invalid_mask = series.notna() & invalid_mask
        return converted, invalid_mask
    return series.astype("string"), pd.Series(False, index=series.index)


def normalize_dataframe_dtypes(
    df: pd.DataFrame,
    dtype_map: Mapping[str, Any],
    rule_id_prefix: str,
    severity: Severity = "critical",
) -> tuple[pd.DataFrame, List[RuleOutcome]]:
    normalized = df.copy()
    outcomes: List[RuleOutcome] = []
    for column, target_dtype in dtype_map.items():
        if column not in normalized.columns:
            continue
        converted, invalid_mask = _coerce_series(normalized[column], target_dtype)
        normalized[column] = converted
        invalid_count = int(invalid_mask.sum())
        passed = invalid_count == 0
        original_samples = (
            df.loc[invalid_mask, column].head(5).apply(_json_safe).tolist()
            if invalid_count
            else []
        )
        description = (
            f"Column '{column}' is parseable as {target_dtype}."
            if passed
            else f"Column '{column}' contains invalid {target_dtype} values."
        )
        outcomes.append(
            build_rule_outcome(
                rule_id=f"{rule_id_prefix}-{column}",
                description=description,
                passed=passed,
                severity=severity if not passed else "info",
                details={
                    "column": column,
                    "target_dtype": str(target_dtype),
                    "invalid_count": invalid_count,
                    "affected_count": invalid_count,
                    "sample_values": original_samples,
                },
            )
        )
    return normalized, outcomes


def detect_duplicates(
    df: pd.DataFrame,
    subset: Sequence[str],
    rule_id: str,
    severity: Severity = "critical",
) -> RuleOutcome:
    subset_columns = list(subset)
    eligible_mask = df[subset_columns].notna().all(axis=1)
    duplicates_mask = eligible_mask & df.duplicated(subset=subset_columns, keep=False)
    duplicate_count = int(duplicates_mask.sum())
    passed = duplicate_count == 0
    sample_values = (
        df.loc[duplicates_mask, subset_columns]
        .drop_duplicates()
        .head(5)
        .to_dict(orient="records")
        if duplicate_count
        else []
    )
    description = (
        "No duplicate rows detected for subset."
        if passed
        else "Duplicate rows detected for subset."
    )
    details = {
        "subset": subset_columns,
        "duplicate_count": duplicate_count,
        "affected_count": duplicate_count,
        "sample_values": sample_values,
    }
    return build_rule_outcome(
        rule_id=rule_id,
        description=description,
        passed=passed,
        severity=severity if not passed else "info",
        details=details,
    )


def validate_uniqueness_constraints(
    df: pd.DataFrame,
    constraints: Sequence[Sequence[str]],
    rule_id_prefix: str,
    severity: Severity = "critical",
) -> List[RuleOutcome]:
    outcomes: List[RuleOutcome] = []
    for index, subset in enumerate(constraints, start=1):
        outcomes.append(
            detect_duplicates(
                df=df,
                subset=subset,
                rule_id=f"{rule_id_prefix}-unique-{index}",
                severity=severity,
            )
        )
    return outcomes


def apply_schema_definition(
    df: pd.DataFrame,
    definition: SchemaDefinition,
    dtype_rule_prefix: str | None = None,
    uniqueness_rule_prefix: str | None = None,
    null_rule_prefix: str | None = None,
) -> tuple[pd.DataFrame, List[RuleOutcome]]:
    """Validate a DataFrame against a schema definition."""

    outcomes: List[RuleOutcome] = []
    outcomes.append(
        check_required_columns(
            df=df,
            required_columns=definition.required_columns,
            rule_id=f"{definition.name}-required-columns",
        )
    )
    null_prefix = null_rule_prefix or f"{definition.name}-nulls"
    outcomes.extend(
        check_non_null_columns(
            df=df,
            columns=definition.non_null_columns,
            rule_id_prefix=null_prefix,
        )
    )
    dtype_prefix = dtype_rule_prefix or f"{definition.name}-dtype"
    normalized_df, dtype_outcomes = normalize_dataframe_dtypes(
        df=df,
        dtype_map=definition.dtype_map,
        rule_id_prefix=dtype_prefix,
    )
    outcomes.extend(dtype_outcomes)
    uniqueness_prefix = uniqueness_rule_prefix or f"{definition.name}-uniqueness"
    outcomes.extend(
        validate_uniqueness_constraints(
            df=normalized_df,
            constraints=definition.uniqueness_constraints,
            rule_id_prefix=uniqueness_prefix,
        )
    )
    return normalized_df, outcomes


__all__ = [
    "RuleOutcome",
    "SchemaDefinition",
    "SCHEMA_DEFINITIONS",
    "SCHEMA_TARGETS",
    "apply_schema_definition",
    "build_rule_outcome",
    "check_non_null_columns",
    "check_required_columns",
    "detect_duplicates",
    "get_schema_definition",
    "normalize_dataframe_dtypes",
    "validate_uniqueness_constraints",
]
