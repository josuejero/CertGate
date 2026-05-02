from __future__ import annotations

import argparse
import json
import os
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

BASE_DIR = Path(__file__).resolve().parents[1]
DEFAULT_REPORTS_DIR = BASE_DIR / "reports"
GX_EXPECTATIONS_DIR = BASE_DIR / "gx" / "expectations"
REGRESSION_DIR = BASE_DIR / "data" / "regression"
SQL_DIR = BASE_DIR / "sql"


def read_json(path: Path, default: Any = None) -> Any:
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8"))


def count_gx_suites_and_expectations() -> tuple[int, int]:
    if not GX_EXPECTATIONS_DIR.exists():
        return 0, 0

    suite_count = 0
    expectation_count = 0
    for path in sorted(GX_EXPECTATIONS_DIR.glob("*_suite.json")):
        suite_count += 1
        payload = read_json(path, {})
        expectation_count += len(payload.get("expectations", []))
    return suite_count, expectation_count


def count_regression_scenarios() -> int:
    if not REGRESSION_DIR.exists():
        return 0
    return sum(1 for path in REGRESSION_DIR.iterdir() if path.is_dir())


def count_sql_reports() -> int:
    if not SQL_DIR.exists():
        return 0
    return len(list(SQL_DIR.glob("*.sql")))


def read_junit_metrics(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {
            "tests": None,
            "failures": None,
            "errors": None,
            "skipped": None,
            "time_seconds": None,
        }

    root = ET.parse(path).getroot()
    # PyTest may emit either <testsuite> or <testsuites> as the root.
    if root.tag == "testsuites":
        test_suites = root.findall("testsuite")
        tests = sum(int(node.attrib.get("tests", 0)) for node in test_suites)
        failures = sum(int(node.attrib.get("failures", 0)) for node in test_suites)
        errors = sum(int(node.attrib.get("errors", 0)) for node in test_suites)
        skipped = sum(int(node.attrib.get("skipped", 0)) for node in test_suites)
        time_seconds = sum(float(node.attrib.get("time", 0)) for node in test_suites)
    else:
        tests = int(root.attrib.get("tests", 0))
        failures = int(root.attrib.get("failures", 0))
        errors = int(root.attrib.get("errors", 0))
        skipped = int(root.attrib.get("skipped", 0))
        time_seconds = float(root.attrib.get("time", 0))

    return {
        "tests": tests,
        "failures": failures,
        "errors": errors,
        "skipped": skipped,
        "time_seconds": round(time_seconds, 3),
    }


def read_coverage_metrics(path: Path) -> dict[str, Any]:
    payload = read_json(path, None)
    if not payload:
        return {
            "line_coverage_percent": None,
            "branch_coverage_percent": None,
            "covered_lines": None,
            "missing_lines": None,
        }

    totals = payload.get("totals", {})
    return {
        "line_coverage_percent": coverage_percent(
            totals,
            (
                "percent_statements_covered",
                "percent_statements_covered_display",
                "percent_covered",
                "percent_covered_display",
            ),
        ),
        "branch_coverage_percent": coverage_percent(
            totals,
            (
                "percent_branches_covered",
                "percent_branches_covered_display",
                "percent_covered_branches",
            ),
        ),
        "covered_lines": totals.get("covered_lines"),
        "missing_lines": totals.get("missing_lines"),
    }


def coverage_percent(totals: dict[str, Any], keys: tuple[str, ...]) -> float | str | None:
    for key in keys:
        value = totals.get(key)
        if value is None:
            continue
        try:
            return round(float(value), 2)
        except (TypeError, ValueError):
            return value
    return None


def percent(numerator: int, denominator: int) -> float | None:
    if denominator == 0:
        return None
    return round(numerator / denominator * 100, 2)


def build_snapshot(reports_dir: Path = DEFAULT_REPORTS_DIR) -> dict[str, Any]:
    validation = read_json(reports_dir / "validation_summary.json", {})
    defects = read_json(reports_dir / "defect_summary.json", {})
    release = read_json(reports_dir / "release_decision.json", {})
    weekly = read_json(reports_dir / "weekly_exec_summary.json", {})
    before = read_json(reports_dir / "demo" / "before" / "defect_summary.json", {})
    after = read_json(reports_dir / "demo" / "after" / "defect_summary.json", {})

    gx_suite_count, gx_expectation_count = count_gx_suites_and_expectations()
    junit = read_junit_metrics(reports_dir / "junit.xml")
    coverage = read_coverage_metrics(reports_dir / "coverage.json")

    records_scanned = int(validation.get("records_scanned", 0))
    rule_count = int(validation.get("rule_count", 0))
    failed_count = int(validation.get("failed_count", 0))
    passed_count = rule_count - failed_count
    defect_count = int(defects.get("defect_count", 0))

    before_defects = int(before.get("defect_count", 0))
    after_defects = int(after.get("defect_count", 0))
    before_critical = int(before.get("severity_counts", {}).get("critical", 0))
    after_critical = int(after.get("severity_counts", {}).get("critical", 0))
    before_warning = int(before.get("severity_counts", {}).get("warning", 0))
    after_warning = int(after.get("severity_counts", {}).get("warning", 0))

    return {
        "project": "CertGate",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "commit_sha": os.getenv("GITHUB_SHA"),
        "run_id": os.getenv("GITHUB_RUN_ID"),
        "data_quality": {
            "tables_validated": len(validation.get("table_counts", {})),
            "table_counts": validation.get("table_counts", {}),
            "records_scanned": records_scanned,
            "rule_count": rule_count,
            "passed_count": passed_count,
            "failed_count": failed_count,
            "rule_pass_rate_percent": percent(passed_count, rule_count),
            "defect_count": defect_count,
            "defects_per_100_records": round(defect_count / records_scanned * 100, 2)
            if records_scanned
            else None,
            "critical_issues": defects.get("severity_counts", {}).get("critical", 0),
            "warning_issues": defects.get("severity_counts", {}).get("warning", 0),
            "release_status": release.get("status"),
        },
        "demo_before_after": {
            "before_defects": before_defects,
            "after_defects": after_defects,
            "defect_reduction": before_defects - after_defects,
            "critical_reduction": before_critical - after_critical,
            "warning_reduction": before_warning - after_warning,
            "baseline_bundle": before.get("bundle_name"),
            "after_bundle": after.get("bundle_name"),
        },
        "great_expectations": {
            "suite_count": gx_suite_count,
            "expectation_count": gx_expectation_count,
            "data_docs_generated": (BASE_DIR / "gx" / "data_docs" / "local_site" / "index.html").exists(),
        },
        "testing": {
            "junit": junit,
            "coverage": coverage,
            "regression_scenarios": count_regression_scenarios(),
        },
        "analytics": {
            "sql_report_count": count_sql_reports(),
            "weekly_exec_summary_generated": bool(weekly),
            "week_over_week_improvement": weekly.get("week_over_week_improvement"),
        },
    }


def markdown_table(snapshot: dict[str, Any]) -> str:
    dq = snapshot["data_quality"]
    demo = snapshot["demo_before_after"]
    gx = snapshot["great_expectations"]
    testing = snapshot["testing"]
    analytics = snapshot["analytics"]
    junit = testing["junit"]
    coverage = testing["coverage"]

    rows = [
        ("Generated at", snapshot["generated_at"]),
        ("Release status", dq["release_status"]),
        ("CRM tables validated", dq["tables_validated"]),
        ("Clean-bundle records scanned", dq["records_scanned"]),
        ("Validation rule outcomes", dq["rule_count"]),
        ("Validation pass rate", format_percent(dq["rule_pass_rate_percent"])),
        ("Clean-bundle failed rules", dq["failed_count"]),
        ("Clean-bundle critical issues", dq["critical_issues"]),
        ("Clean-bundle warning issues", dq["warning_issues"]),
        ("Before-cleanup defects", demo["before_defects"]),
        ("After-cleanup defects", demo["after_defects"]),
        ("Defect reduction", demo["defect_reduction"]),
        ("Critical defect reduction", demo["critical_reduction"]),
        ("Warning defect reduction", demo["warning_reduction"]),
        ("Regression scenario bundles", testing["regression_scenarios"]),
        ("Great Expectations suites", gx["suite_count"]),
        ("Great Expectations expectations", gx["expectation_count"]),
        ("Data Docs generated", gx["data_docs_generated"]),
        ("DuckDB SQL insight reports", analytics["sql_report_count"]),
        ("PyTest tests", junit["tests"]),
        ("PyTest failures", junit["failures"]),
        ("PyTest errors", junit["errors"]),
        ("PyTest skipped", junit["skipped"]),
        ("Test duration seconds", junit["time_seconds"]),
        ("Line coverage", format_percent(coverage["line_coverage_percent"])),
        ("Branch coverage", format_percent(coverage["branch_coverage_percent"])),
    ]

    lines = ["# CertGate metrics snapshot", "", "| Metric | Value |", "|---|---:|"]
    for label, value in rows:
        if value is None:
            value = "Not generated"
        lines.append(f"| {label} | {value} |")

    lines.extend(
        [
            "",
            "## Employer-facing summary",
            "",
            f"CertGate validates {dq['records_scanned']} CRM records across {dq['tables_validated']} tables with {dq['rule_count']} validation rule outcomes and a {dq['release_status']} release decision on the clean bundle.",
            "",
            f"The before/after demo shows {demo['before_defects']} seeded CRM defects reduced to {demo['after_defects']} after cleanup, including {demo['critical_reduction']} critical defects and {demo['warning_reduction']} warning defects removed.",
        ]
    )
    return "\n".join(lines) + "\n"


def format_percent(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value)
    if text.endswith("%"):
        return text
    return f"{text}%"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate CertGate machine-readable and Markdown metrics snapshots."
    )
    parser.add_argument(
        "--reports-dir",
        type=Path,
        default=DEFAULT_REPORTS_DIR,
        help="Directory containing generated report artifacts.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    reports_dir = args.reports_dir.resolve()
    reports_dir.mkdir(parents=True, exist_ok=True)
    snapshot = build_snapshot(reports_dir)
    (reports_dir / "metrics_snapshot.json").write_text(
        json.dumps(snapshot, indent=2), encoding="utf-8"
    )
    (reports_dir / "metrics_snapshot.md").write_text(markdown_table(snapshot), encoding="utf-8")
    print(
        f"Wrote {reports_dir / 'metrics_snapshot.json'} and {reports_dir / 'metrics_snapshot.md'}"
    )


if __name__ == "__main__":
    main()
