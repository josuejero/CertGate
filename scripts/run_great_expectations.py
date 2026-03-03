from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

SUPPORTED_PYTHON_MIN = (3, 10)
SUPPORTED_PYTHON_MAX = (3, 13)


def _ensure_python_supported() -> None:
    version = sys.version_info[:2]
    if SUPPORTED_PYTHON_MIN <= version <= SUPPORTED_PYTHON_MAX:
        return
    version_str = f"{version[0]}.{version[1]}"
    base_message = (
        "Great Expectations (<0.18) is tested on Python 3.10→3.13, "
        f"but you are running Python {version_str}."
    )
    if version > SUPPORTED_PYTHON_MAX:
        raise SystemExit(
            base_message
            + " Python 3.14+ is not supported yet because the bundled Pydantic v1 dependency "
            "issues `issubclass` checks against typing generics and raises "
            "\"Subscripted generics cannot be used with class and instance checks\" "
            "on Python 3.14+. Recreate your `.venv` with a supported interpreter (for example "
            "`PYTHON_CMD=python3.13 ./scripts/bootstrap.sh`) before rerunning this script."
        )
    raise SystemExit(base_message + " Please use a supported interpreter before rerunning this script.")


_ensure_python_supported()

import pandas as pd
from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.data_context import DataContext

from src.ingest.loaders import load_table
from src.rules.schema_rules import get_schema_definition

DATA_ROOT = BASE_DIR / "data" / "good"
GX_ROOT = BASE_DIR / "gx"

SCHEMA_TARGETS = (
    "candidates",
    "exam_results",
    "certification_status",
)

ACTION_LIST = [
    {
        "name": "store_validation_result",
        "action": {"class_name": "StoreValidationResultAction"},
    },
    {
        "name": "store_evaluation_parameters",
        "action": {"class_name": "StoreEvaluationParametersAction"},
    },
    {
        "name": "update_data_docs",
        "action": {"class_name": "UpdateDataDocsAction"},
    },
]


def _load_dataframe(table_key: str) -> pd.DataFrame:
    definition = get_schema_definition(table_key)
    table_path = DATA_ROOT / definition.file_name
    loaded = load_table(
        name=table_key,
        path=table_path,
        parse_dates=definition.parse_dates,
        dtype_overrides=definition.dtype_map,
    )
    return loaded.df


def _to_datetime(value: pd.Timestamp | datetime | str) -> datetime:
    if pd.isna(value):
        raise ValueError("Timestamp value is missing")
    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime()
    if isinstance(value, str):
        return pd.to_datetime(value).to_pydatetime()
    if isinstance(value, datetime):
        return value
    raise ValueError(f"Unable to interpret {value!r} as datetime")  # type: ignore[unreachable]


def _build_batch_request(df: pd.DataFrame, identifier: str) -> RuntimeBatchRequest:
    return RuntimeBatchRequest(
        datasource_name="pandas_ingest",
        data_connector_name="runtime_data_connector",
        data_asset_name="ingest_batch",
        runtime_parameters={"batch_data": df},
        batch_identifiers={"ingest_batch": identifier},
    )


def main() -> None:
    context = DataContext(context_root_dir=str(GX_ROOT))

    candidates_df = _load_dataframe("candidates")
    exam_df = _load_dataframe("exam_results")
    cert_df = _load_dataframe("certification_status")

    evaluation_params = {
        "min_exam_date": _to_datetime(exam_df["exam_date"].min()),
        "max_exam_date": _to_datetime(exam_df["exam_date"].max()),
        "min_file_received_ts": _to_datetime(exam_df["file_received_ts"].min()),
        "max_file_received_ts": _to_datetime(exam_df["file_received_ts"].max()),
    }

    validations = [
        {
            "batch_request": _build_batch_request(candidates_df, "candidates"),
            "expectation_suite_name": "candidates_suite",
        },
        {
            "batch_request": _build_batch_request(exam_df, "exams"),
            "expectation_suite_name": "exam_results_suite",
        },
        {
            "batch_request": _build_batch_request(cert_df, "certifications"),
            "expectation_suite_name": "cert_status_suite",
        },
        {
            "batch_request": _build_batch_request(exam_df, "freshness"),
            "expectation_suite_name": "freshness_suite",
        },
    ]

    result = context.run_checkpoint(
        checkpoint_name="ingest_validation_checkpoint",
        validations=validations,
        evaluation_parameters=evaluation_params,
        action_list=ACTION_LIST,
    )
    print("Great Expectations checkpoint executed:", result.name)


if __name__ == "__main__":
    main()
