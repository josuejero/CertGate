"""DuckDB-backed analytics and executive-summary exports."""

from __future__ import annotations

import json
from collections import defaultdict
from pathlib import Path
from typing import Any, Mapping

import pandas as pd

from certgate.ingest.loaders import LoadedTable
from certgate.reporting import ReleaseReport

OPS_QUERY_NAMES = (
    "duplicate_domains",
    "stale_opportunities",
    "owner_workload",
    "missing_firmographics",
    "conversion_snapshot",
    "weekly_exec_summary",
)


def _records_from_dataframe(df: pd.DataFrame) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for row in df.to_dict(orient="records"):
        cleaned = {}
        for key, value in row.items():
            if isinstance(value, pd.Timestamp):
                cleaned[key] = value.isoformat()
            elif hasattr(value, "item"):
                try:
                    cleaned[key] = value.item()
                except (TypeError, ValueError):
                    cleaned[key] = str(value)
            else:
                cleaned[key] = value
        records.append(cleaned)
    return records


def _table_markdown(title: str, records: list[dict[str, Any]]) -> str:
    if not records:
        return f"# {title}\n\nNo rows returned.\n"
    columns = list(records[0].keys())
    header = "| " + " | ".join(columns) + " |"
    separator = "| " + " | ".join("---" for _ in columns) + " |"
    rows = []
    for record in records:
        rows.append("| " + " | ".join(str(record.get(column, "")) for column in columns) + " |")
    body = "\n".join(rows)
    return f"# {title}\n\n{header}\n{separator}\n{body}\n"


def run_ops_insights(
    tables: Mapping[str, LoadedTable],
    sql_dir: Path | str,
    output_dir: Path | str,
) -> dict[str, list[dict[str, Any]]]:
    """Execute DuckDB SQL insights and write JSON/Markdown artifacts."""

    import duckdb

    sql_root = Path(sql_dir)
    destination = Path(output_dir)
    destination.mkdir(parents=True, exist_ok=True)

    first_table = next(iter(tables.values()))
    context_df = pd.DataFrame(
        [
            {
                "bundle_name": first_table.metadata.get("bundle_name", "good"),
                "reference_time": first_table.metadata.get("bundle_reference_time"),
            }
        ]
    )

    results: dict[str, list[dict[str, Any]]] = {}
    connection = duckdb.connect()
    try:
        for name, table in tables.items():
            connection.register(name, table.df)
        connection.register("bundle_context", context_df)

        for query_name in OPS_QUERY_NAMES:
            sql_text = (sql_root / f"{query_name}.sql").read_text()
            query_df = connection.execute(sql_text).df()
            records = _records_from_dataframe(query_df)
            results[query_name] = records
            (destination / f"{query_name}.json").write_text(json.dumps(records, indent=2))
            (destination / f"{query_name}.md").write_text(
                _table_markdown(query_name.replace("_", " ").title(), records)
            )
    finally:
        connection.close()

    return results


def _find_failure_count(report: ReleaseReport, rule_id: str) -> int:
    for outcome in report.failing_outcomes:
        if outcome.rule_id == rule_id:
            return int(outcome.details.get("affected_count", 0))
    return 0


def _owner_recommendations(
    report: ReleaseReport,
    owners_df: pd.DataFrame,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    owner_to_team = owners_df.set_index("owner_name")["team"].to_dict()
    owner_counts: defaultdict[str, int] = defaultdict(int)
    for entry in report.top_affected_owners:
        owner_counts[entry["name"]] += int(entry["count"])

    owner_actions = []
    team_counts: defaultdict[str, int] = defaultdict(int)
    for owner_name, count in sorted(owner_counts.items(), key=lambda item: (-item[1], item[0])):
        team = str(owner_to_team.get(owner_name, "Unassigned"))
        team_counts[team] += count
        owner_actions.append(
            {
                "owner_name": owner_name,
                "team": team,
                "issue_count": count,
                "recommended_action": "Review stale follow-up, dedupe, and routing defects for this owner.",
            }
        )

    team_actions = [
        {
            "team": team,
            "issue_count": count,
            "recommended_action": "Prioritize routing fixes and follow-up hygiene for this team.",
        }
        for team, count in sorted(team_counts.items(), key=lambda item: (-item[1], item[0]))
    ]
    return owner_actions, team_actions


def build_weekly_exec_summary(
    report: ReleaseReport,
    insights: Mapping[str, list[dict[str, Any]]],
    owners_df: pd.DataFrame,
    output_dir: Path | str,
    baseline_report: ReleaseReport | None = None,
    baseline_insights: Mapping[str, list[dict[str, Any]]] | None = None,
) -> dict[str, Any]:
    """Write the weekly executive summary JSON/Markdown pair."""

    destination = Path(output_dir)
    destination.mkdir(parents=True, exist_ok=True)

    owner_actions, team_actions = _owner_recommendations(report, owners_df)
    summary = {
        "bundle_name": report.bundle_name,
        "generated_at": report.iso_timestamp,
        "records_scanned": report.records_scanned,
        "critical_issues": report.severity_counts["critical"],
        "warning_issues": report.severity_counts["warning"],
        "duplicate_domains": len(insights.get("duplicate_domains", [])),
        "stale_opportunities_over_threshold": len(insights.get("stale_opportunities", [])),
        "open_opportunities_with_no_next_step": _find_failure_count(
            report, "crm-open-opportunity-next-step"
        ),
        "inactive_owner_mappings": _find_failure_count(
            report, "crm-inactive-owner-open-opportunity"
        ),
        "recommended_actions_by_owner": owner_actions,
        "recommended_actions_by_team": team_actions,
        "week_over_week_improvement": None,
    }

    if baseline_report is not None and baseline_insights is not None:
        summary["week_over_week_improvement"] = {
            "baseline_bundle": baseline_report.bundle_name,
            "critical_issue_reduction": baseline_report.severity_counts["critical"]
            - report.severity_counts["critical"],
            "warning_issue_reduction": baseline_report.severity_counts["warning"]
            - report.severity_counts["warning"],
            "duplicate_domain_reduction": len(baseline_insights.get("duplicate_domains", []))
            - len(insights.get("duplicate_domains", [])),
            "stale_opportunity_reduction": len(
                baseline_insights.get("stale_opportunities", [])
            )
            - len(insights.get("stale_opportunities", [])),
        }

    markdown_lines = [
        "# Weekly Executive Summary",
        "",
        f"- Bundle: {summary['bundle_name']}",
        f"- Records scanned: {summary['records_scanned']}",
        f"- Critical issues: {summary['critical_issues']}",
        f"- Warning issues: {summary['warning_issues']}",
        f"- Duplicate domains: {summary['duplicate_domains']}",
        f"- Stale opportunities over threshold: {summary['stale_opportunities_over_threshold']}",
        f"- Open opportunities with no next step: {summary['open_opportunities_with_no_next_step']}",
        f"- Inactive owner mappings: {summary['inactive_owner_mappings']}",
        "",
        "## Recommended Actions By Owner",
        "",
    ]

    if owner_actions:
        markdown_lines.extend(
            [
                f"- {entry['owner_name']} ({entry['team']}): {entry['issue_count']} issue(s). {entry['recommended_action']}"
                for entry in owner_actions
            ]
        )
    else:
        markdown_lines.append("- No owner-level follow-up required.")

    markdown_lines.extend(["", "## Recommended Actions By Team", ""])
    if team_actions:
        markdown_lines.extend(
            [
                f"- {entry['team']}: {entry['issue_count']} issue(s). {entry['recommended_action']}"
                for entry in team_actions
            ]
        )
    else:
        markdown_lines.append("- No team-level follow-up required.")

    if summary["week_over_week_improvement"] is not None:
        improvement = summary["week_over_week_improvement"]
        markdown_lines.extend(
            [
                "",
                "## Week Over Week Improvement",
                "",
                f"- Baseline bundle: {improvement['baseline_bundle']}",
                f"- Critical issue reduction: {improvement['critical_issue_reduction']}",
                f"- Warning issue reduction: {improvement['warning_issue_reduction']}",
                f"- Duplicate domain reduction: {improvement['duplicate_domain_reduction']}",
                f"- Stale opportunity reduction: {improvement['stale_opportunity_reduction']}",
            ]
        )

    (destination / "weekly_exec_summary.json").write_text(json.dumps(summary, indent=2))
    (destination / "weekly_exec_summary.md").write_text("\n".join(markdown_lines) + "\n")
    return summary


__all__ = [
    "OPS_QUERY_NAMES",
    "build_weekly_exec_summary",
    "run_ops_insights",
]
