from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING

BASE_DIR = Path(__file__).resolve().parents[1]
SRC_ROOT = BASE_DIR / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

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
            "issues `issubclass` checks against typing generics. Recreate your `.venv` with "
            "a supported interpreter (for example `PYTHON_CMD=python3.13 ./scripts/bootstrap.sh`) "
            "before rerunning this script."
        )
    raise SystemExit(base_message + " Please use a supported interpreter before rerunning this script.")


import pandas as pd

if TYPE_CHECKING:
    from great_expectations.core.batch import RuntimeBatchRequest

from certgate.ingest import load_bundle

DATA_ROOT = BASE_DIR / "data"
GX_ROOT = BASE_DIR / "gx"

SCHEMA_TARGETS = (
    "leads",
    "accounts",
    "opportunities",
    "activities",
    "owners",
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

VALIDATION_RUN_CONFIGS = (
    {
        "table_key": "leads",
        "expectation_suite_name": "leads_suite",
        "batch_identifier": "leads",
    },
    {
        "table_key": "accounts",
        "expectation_suite_name": "accounts_suite",
        "batch_identifier": "accounts",
    },
    {
        "table_key": "opportunities",
        "expectation_suite_name": "opportunities_suite",
        "batch_identifier": "opportunities",
    },
    {
        "table_key": "activities",
        "expectation_suite_name": "activities_suite",
        "batch_identifier": "activities",
    },
    {
        "table_key": "owners",
        "expectation_suite_name": "owners_suite",
        "batch_identifier": "owners",
    },
    {
        "table_key": "opportunities",
        "expectation_suite_name": "freshness_suite",
        "batch_identifier": "freshness",
    },
)


def _build_batch_request(df: pd.DataFrame, identifier: str) -> RuntimeBatchRequest:
    from great_expectations.core.batch import RuntimeBatchRequest

    return RuntimeBatchRequest(
        datasource_name="pandas_ingest",
        data_connector_name="runtime_data_connector",
        data_asset_name="ingest_batch",
        runtime_parameters={"batch_data": df},
        batch_identifiers={"ingest_batch": identifier},
    )


def _bundle_time_bounds(tables: dict[str, pd.DataFrame]) -> tuple[datetime, datetime]:
    timestamps: list[pd.Timestamp] = []
    for table_name in ("leads", "accounts", "opportunities", "activities"):
        frame = tables[table_name]
        for column in frame.columns:
            if not column.endswith("_at") and not column.endswith("_date"):
                continue
            series = pd.to_datetime(frame[column], utc=True, errors="coerce").dropna()
            if not series.empty:
                timestamps.append(series.min())
                timestamps.append(series.max())

    if not timestamps:
        raise ValueError("Unable to derive bundle time bounds for Great Expectations.")
    return min(timestamps).to_pydatetime(), max(timestamps).to_pydatetime()


def main() -> None:
    _ensure_python_supported()
    from great_expectations.data_context import DataContext

    context = DataContext(context_root_dir=str(GX_ROOT))
    loaded_tables = load_bundle(DATA_ROOT, "good", targets=SCHEMA_TARGETS)
    table_dfs = {name: table.df for name, table in loaded_tables.items()}
    bundle_min_timestamp, bundle_max_timestamp = _bundle_time_bounds(table_dfs)

    evaluation_params = {
        "bundle_min_timestamp": bundle_min_timestamp,
        "bundle_max_timestamp": bundle_max_timestamp,
    }

    validations = [
        {
            "batch_request": _build_batch_request(
                table_dfs[config["table_key"]], config["batch_identifier"]
            ),
            "expectation_suite_name": config["expectation_suite_name"],
        }
        for config in VALIDATION_RUN_CONFIGS
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
