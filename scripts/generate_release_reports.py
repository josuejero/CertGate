from __future__ import annotations

from pathlib import Path

from src.ingest.loaders import load_table
from src.reporting import ReleaseReport
from src.rules.business_rules import (
    check_cert_candidate_fk,
    check_exam_candidate_fk,
    check_pass_fail_certification,
    evaluate_table_freshness,
)
from src.rules.schema_rules import apply_schema_definition, get_schema_definition

BASE_DIR = Path(__file__).resolve().parents[1]
DATA_ROOT = BASE_DIR / "data" / "good"
REPORT_DIR = BASE_DIR / "reports"
SCHEMA_TARGETS = ("candidates", "exam_results", "certification_status")


def _load_table(name: str):
    schema = get_schema_definition(name)
    return load_table(
        name=name,
        path=DATA_ROOT / schema.file_name,
        parse_dates=schema.parse_dates,
        dtype_overrides=schema.dtype_map,
    )


def main() -> None:
    bundle = {name: _load_table(name) for name in SCHEMA_TARGETS}
    outcomes = []
    for name in SCHEMA_TARGETS:
        definition = get_schema_definition(name)
        _, schema_outcomes = apply_schema_definition(bundle[name].df, definition)
        outcomes.extend(schema_outcomes)

    exam_table = bundle["exam_results"].df
    candidates_table = bundle["candidates"].df
    cert_table = bundle["certification_status"].df

    outcomes.append(check_exam_candidate_fk(exam_table, candidates_table))
    outcomes.append(check_cert_candidate_fk(cert_table, candidates_table))
    outcomes.extend(check_pass_fail_certification(exam_table, cert_table))
    outcomes.append(
        evaluate_table_freshness(
            bundle["exam_results"],
            timestamp_column="file_received_ts",
            max_allowed_hours=24,
            warning_hours=48,
        )
    )

    report = ReleaseReport(outcomes)
    report.write_reports(REPORT_DIR)
    print("Release reports written to", REPORT_DIR)


if __name__ == "__main__":
    main()
