"""Release gate exports and machine-readable summaries."""

from __future__ import annotations

import json
import re
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

from certgate.rules.schema import RuleOutcome

STATUS_READY = "Ready"
STATUS_WARNING_ONLY = "Warning only"
STATUS_BLOCKED = "Blocked"

ROOT_CAUSE_SCHEMA = "schema_issue"
ROOT_CAUSE_INTEGRITY = "integrity_issue"
ROOT_CAUSE_STALE_DATA = "stale_data"
ROOT_CAUSE_DUPLICATE = "duplicate_record"
ROOT_CAUSE_INVALID_BUSINESS_RULE = "invalid_business_rule_state"

REMEDIATION_BASE = "docs/rule-severity-matrix.md"
_SLUGIFY_PATTERN = re.compile(r"[^a-z0-9-]+")


def _slugify_rule_id(rule_id: str) -> str:
    slug = rule_id.strip().lower().replace("_", "-").replace(" ", "-")
    slug = _SLUGIFY_PATTERN.sub("", slug)
    return slug.strip("-") or rule_id.lower()


def _remediation_link(rule_id: str) -> str:
    return f"{REMEDIATION_BASE}#{_slugify_rule_id(rule_id)}"


def infer_root_cause(outcome: RuleOutcome) -> str:
    rule = outcome.rule_id.lower()
    if any(token in rule for token in ("required-columns", "-dtype-", "-nulls-", "values")):
        return ROOT_CAUSE_SCHEMA
    if any(token in rule for token in ("duplicate", "uniqueness", "multiple-accounts", "domain-multiple")):
        return ROOT_CAUSE_DUPLICATE
    if any(token in rule for token in ("-fk", "reference", "domain-mismatch")):
        return ROOT_CAUSE_INTEGRITY
    if any(token in rule for token in ("stale", "recent-touch", "freshness")):
        return ROOT_CAUSE_STALE_DATA
    return ROOT_CAUSE_INVALID_BUSINESS_RULE


def _sum_named_counts(outcomes: Sequence[RuleOutcome], detail_key: str) -> list[dict[str, Any]]:
    counts: defaultdict[str, int] = defaultdict(int)
    for outcome in outcomes:
        detail_counts = outcome.details.get(detail_key, {})
        if not isinstance(detail_counts, Mapping):
            continue
        for name, count in detail_counts.items():
            counts[str(name)] += int(count)
    return [
        {"name": name, "count": count}
        for name, count in sorted(counts.items(), key=lambda item: (-item[1], item[0]))
    ]


def _severity_counts(outcomes: Sequence[RuleOutcome]) -> dict[str, int]:
    counts = Counter(outcome.severity for outcome in outcomes if not outcome.passed)
    return {
        "critical": int(counts.get("critical", 0)),
        "warning": int(counts.get("warning", 0)),
        "info": int(counts.get("info", 0)),
    }


@dataclass
class ReleaseReport:
    """Encapsulates release gating summaries and exports."""

    outcomes: Sequence[RuleOutcome]
    bundle_name: str = "good"
    table_counts: Mapping[str, int] = field(default_factory=dict)
    timestamp: datetime | None = None

    def __post_init__(self) -> None:
        if self.timestamp is None:
            self.timestamp = datetime.now(timezone.utc)
        self._outcomes = list(self.outcomes)

    @property
    def failing_outcomes(self) -> list[RuleOutcome]:
        return [outcome for outcome in self._outcomes if not outcome.passed]

    @property
    def iso_timestamp(self) -> str:
        return self.timestamp.isoformat()

    @property
    def records_scanned(self) -> int:
        return int(sum(self.table_counts.values()))

    @property
    def severity_counts(self) -> dict[str, int]:
        return _severity_counts(self._outcomes)

    @property
    def top_affected_owners(self) -> list[dict[str, Any]]:
        return _sum_named_counts(self.failing_outcomes, "owner_issue_counts")[:5]

    @property
    def top_affected_stages(self) -> list[dict[str, Any]]:
        return _sum_named_counts(self.failing_outcomes, "stage_issue_counts")[:5]

    @property
    def status(self) -> str:
        failing = self.failing_outcomes
        if any(outcome.severity == "critical" for outcome in failing):
            return STATUS_BLOCKED
        if failing:
            return STATUS_WARNING_ONLY
        return STATUS_READY

    def _rule_payload(self, outcome: RuleOutcome) -> dict[str, Any]:
        return {
            "rule_id": outcome.rule_id,
            "description": outcome.description,
            "passed": outcome.passed,
            "severity": outcome.severity,
            "root_cause": infer_root_cause(outcome),
            "remediation_link": _remediation_link(outcome.rule_id),
            "details": outcome.details or {},
        }

    def validation_summary(self) -> dict[str, Any]:
        failing = self.failing_outcomes
        return {
            "generated_at": self.iso_timestamp,
            "bundle_name": self.bundle_name,
            "table_counts": dict(self.table_counts),
            "records_scanned": self.records_scanned,
            "rule_count": len(self._outcomes),
            "failed_count": len(failing),
            "severity_counts": self.severity_counts,
            "top_affected_owners": self.top_affected_owners,
            "top_affected_stages": self.top_affected_stages,
            "rules": [self._rule_payload(outcome) for outcome in self._outcomes],
        }

    def defect_summary(self) -> dict[str, Any]:
        defects = [self._rule_payload(outcome) for outcome in self.failing_outcomes]
        counts = Counter(entry["root_cause"] for entry in defects)
        return {
            "generated_at": self.iso_timestamp,
            "bundle_name": self.bundle_name,
            "table_counts": dict(self.table_counts),
            "records_scanned": self.records_scanned,
            "defect_count": len(defects),
            "severity_counts": self.severity_counts,
            "top_affected_owners": self.top_affected_owners,
            "top_affected_stages": self.top_affected_stages,
            "defects": defects,
            "root_cause_counts": dict(counts),
        }

    def release_decision(self) -> dict[str, Any]:
        failing = self.failing_outcomes
        status = self.status
        decision_reason = self._decision_reason(status, failing)
        payload = {
            "status": status,
            "timestamp": self.iso_timestamp,
            "bundle_name": self.bundle_name,
            "records_scanned": self.records_scanned,
            "table_counts": dict(self.table_counts),
            "severity_counts": self.severity_counts,
            "top_affected_owners": self.top_affected_owners,
            "top_affected_stages": self.top_affected_stages,
            "decision_reason": decision_reason,
            "failed_rules": [
                {
                    "rule_id": outcome.rule_id,
                    "severity": outcome.severity,
                    "root_cause": infer_root_cause(outcome),
                    "details": outcome.details or {},
                }
                for outcome in failing
            ],
        }
        if status == STATUS_BLOCKED:
            critical_root_causes = sorted(
                {
                    infer_root_cause(outcome)
                    for outcome in failing
                    if outcome.severity == "critical"
                }
            )
            if critical_root_causes:
                payload["blocking_root_causes"] = critical_root_causes
        return payload

    @staticmethod
    def _decision_reason(status: str, failing: list[RuleOutcome]) -> str:
        if status == STATUS_BLOCKED:
            critical_failure = next(
                (outcome for outcome in failing if outcome.severity == "critical"),
                None,
            )
            if critical_failure is not None:
                return f"Critical failure {critical_failure.rule_id} blocks CRM sync."
            return "Critical data quality failures block CRM sync."
        if status == STATUS_WARNING_ONLY:
            first_failure = failing[0] if failing else None
            if first_failure is not None:
                return f"Non-blocking issue {first_failure.rule_id} requires follow-up."
            return "Warnings were raised; CRM sync may proceed with follow-up."
        return "All validations passed; CRM sync is ready."

    def write_reports(self, directory: Path | str = Path("reports")) -> None:
        base = Path(directory)
        base.mkdir(parents=True, exist_ok=True)
        payloads = [
            ("validation_summary.json", self.validation_summary()),
            ("defect_summary.json", self.defect_summary()),
            ("release_decision.json", self.release_decision()),
        ]
        for name, payload in payloads:
            (base / name).write_text(json.dumps(payload, indent=2))


@dataclass(frozen=True)
class ReportWriter:
    """Helper that writes every CertGate report payload to disk."""

    directory: Path | str = Path("reports")

    def write(self, report: ReleaseReport) -> None:
        report.write_reports(self.directory)


__all__ = [
    "ROOT_CAUSE_DUPLICATE",
    "ROOT_CAUSE_INTEGRITY",
    "ROOT_CAUSE_INVALID_BUSINESS_RULE",
    "ROOT_CAUSE_SCHEMA",
    "ROOT_CAUSE_STALE_DATA",
    "ReleaseReport",
    "ReportWriter",
    "STATUS_BLOCKED",
    "STATUS_READY",
    "STATUS_WARNING_ONLY",
    "infer_root_cause",
]
