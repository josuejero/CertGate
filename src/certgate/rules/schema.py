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
    dtype_map: Mapping[str, Any]
    parse_dates: Tuple[str, ...]
    uniqueness_constraints: Tuple[Tuple[str, ...], ...] = field(default_factory=tuple)


SCHEMA_DEFINITIONS: Dict[str, SchemaDefinition] = {
    "candidates": SchemaDefinition(
        name="candidates",
        file_name="candidates.csv",
        required_columns=(
            "candidate_id",
            "birth_date",
            "email",
            "first_name",
            "last_name",
            "created_ts",
            "source_system",
        ),
        parse_dates=("birth_date", "created_ts"),
        dtype_map={
            "candidate_id": "string",
            "birth_date": "datetime64[ns]",
            "email": "string",
            "first_name": "string",
            "last_name": "string",
            "created_ts": "datetime64[ns]",
            "source_system": "string",
        },
        uniqueness_constraints=(("candidate_id",), ("email",)),
    ),
    "exam_results": SchemaDefinition(
        name="exam_results",
        file_name="exam_results.csv",
        required_columns=(
            "exam_result_id",
            "candidate_id",
            "exam_code",
            "exam_date",
            "attempt_number",
            "score",
            "score_scaled",
            "pass_fail",
            "exam_location",
            "file_received_ts",
        ),
        parse_dates=("exam_date", "file_received_ts"),
        dtype_map={
            "exam_result_id": "string",
            "candidate_id": "string",
            "exam_code": "string",
            "exam_date": "datetime64[ns]",
            "attempt_number": "Int64",
            "score": "float64",
            "score_scaled": "float64",
            "pass_fail": "string",
            "exam_location": "string",
            "file_received_ts": "datetime64[ns]",
        },
        uniqueness_constraints=(
            ("exam_result_id",),
            ("candidate_id", "exam_code", "attempt_number"),
        ),
    ),
    "certification_status": SchemaDefinition(
        name="certification_status",
        file_name="certification_status.csv",
        required_columns=(
            "cert_status_id",
            "candidate_id",
            "certification_level",
            "certification_status",
            "certified_ts",
            "status_source",
            "status_effective_ts",
        ),
        parse_dates=("certified_ts", "status_effective_ts"),
        dtype_map={
            "cert_status_id": "string",
            "candidate_id": "string",
            "certification_level": "string",
            "certification_status": "string",
            "certified_ts": "datetime64[ns]",
            "status_source": "string",
            "status_effective_ts": "datetime64[ns]",
        },
        uniqueness_constraints=(
            ("cert_status_id",),
            ("candidate_id", "certification_level"),
        ),
    ),
}

SCHEMA_TARGETS: Tuple[str, ...] = tuple(SCHEMA_DEFINITIONS.keys())

__all__ = [
    "RuleOutcome",
    "SchemaDefinition",
    "SCHEMA_DEFINITIONS",
    "SCHEMA_TARGETS",
    "build_rule_outcome",
    "get_schema_definition",
    "check_required_columns",
    "normalize_dataframe_dtypes",
    "detect_duplicates",
    "validate_uniqueness_constraints",
    "apply_schema_definition",
]


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
        details=dict(details or {}),
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
    details = {"missing_columns": missing} if missing else {}
    return build_rule_outcome(
        rule_id=rule_id,
        description=description,
        passed=passed,
        severity=severity,
        details=details,
    )


def normalize_dataframe_dtypes(
    df: pd.DataFrame,
    dtype_map: Mapping[str, Any],
    rule_id_prefix: str,
    severity: Severity = "warning",
) -> tuple[pd.DataFrame, List[RuleOutcome]]:
    normalized = df.copy()
    issues: List[RuleOutcome] = []
    for column, target_dtype in dtype_map.items():
        if column not in normalized.columns:
            continue
        try:
            normalized[column] = normalized[column].astype(target_dtype)
        except (TypeError, ValueError) as exc:
            fallback = str(target_dtype).lower()
            if "datetime" in fallback:
                normalized[column] = pd.to_datetime(normalized[column], errors="coerce")
            else:
                normalized[column] = normalized[column].astype("object")
            issues.append(
                build_rule_outcome(
                    rule_id=f"{rule_id_prefix}-{column}",
                    description=f"Could not coerce {column} to {target_dtype}.",
                    passed=False,
                    severity=severity,
                    details={"column": column, "target_dtype": str(target_dtype), "error": str(exc)},
                )
            )
    return normalized, issues


def detect_duplicates(
    df: pd.DataFrame,
    subset: Sequence[str],
    rule_id: str,
    severity: Severity = "critical",
) -> RuleOutcome:
    duplicates_mask = df.duplicated(subset=list(subset), keep=False)
    duplicate_count = int(duplicates_mask.sum())
    passed = duplicate_count == 0
    description = (
        "No duplicate rows detected for subset." if passed else "Duplicate rows detected."
    )
    details = {
        "subset": list(subset),
        "duplicate_count": duplicate_count,
    }
    return build_rule_outcome(
        rule_id=rule_id,
        description=description,
        passed=passed,
        severity=severity,
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
    dtype_prefix = dtype_rule_prefix or f"{definition.name}-dtype"
    normalized, dtype_issues = normalize_dataframe_dtypes(
        df=df,
        dtype_map=definition.dtype_map,
        rule_id_prefix=dtype_prefix,
    )
    outcomes.extend(dtype_issues)
    if definition.uniqueness_constraints:
        unique_prefix = uniqueness_rule_prefix or f"{definition.name}-uniqueness"
        outcomes.extend(
            validate_uniqueness_constraints(
                df=normalized,
                constraints=definition.uniqueness_constraints,
                rule_id_prefix=unique_prefix,
            )
        )
    return normalized, outcomes
