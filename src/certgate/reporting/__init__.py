"""Release gate exports and machine-readable summaries."""

from __future__ import annotations

import json
import re
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Sequence

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
    slug = _slugify_rule_id(rule_id)
    return f"{REMEDIATION_BASE}#{slug}"


def infer_root_cause(outcome: RuleOutcome) -> str:
    rule = outcome.rule_id.lower()
    if "required-columns" in rule or "-dtype" in rule or "schema" in rule:
        return ROOT_CAUSE_SCHEMA
    if "-uniqueness" in rule or "duplicate" in rule or rule.startswith("br-09"):
        return ROOT_CAUSE_DUPLICATE
    if rule.startswith("br-07") or "fk" in rule or "integrity" in rule:
        return ROOT_CAUSE_INTEGRITY
    if rule.startswith("br-08") or "freshness" in rule:
        return ROOT_CAUSE_STALE_DATA
    if rule.startswith("br-05") or rule.startswith("br-06"):
        return ROOT_CAUSE_INVALID_BUSINESS_RULE
    return ROOT_CAUSE_INVALID_BUSINESS_RULE


@dataclass
class ReleaseReport:
    """Encapsulates release gating summaries and exports."""

    outcomes: Sequence[RuleOutcome]
    timestamp: datetime | None = None

    def __post_init__(self) -> None:
        if self.timestamp is None:
            self.timestamp = datetime.now(timezone.utc)
        # Normalize sequence to list for repeated iteration
        self._outcomes = list(self.outcomes)

    @property
    def _failing_outcomes(self) -> list[RuleOutcome]:
        return [outcome for outcome in self._outcomes if not outcome.passed]

    @property
    def iso_timestamp(self) -> str:
        return self.timestamp.isoformat()

    @property
    def status(self) -> str:
        failing = self._failing_outcomes
        if any(outcome.severity == "critical" for outcome in failing):
            return STATUS_BLOCKED
        if failing:
            return STATUS_WARNING_ONLY
        return STATUS_READY

    def _rule_payload(self, outcome: RuleOutcome) -> dict:
        return {
            "rule_id": outcome.rule_id,
            "description": outcome.description,
            "passed": outcome.passed,
            "severity": outcome.severity,
            "root_cause": infer_root_cause(outcome),
            "remediation_link": _remediation_link(outcome.rule_id),
            "details": outcome.details or {},
        }

    def validation_summary(self) -> dict:
        failing = self._failing_outcomes
        return {
            "generated_at": self.iso_timestamp,
            "rule_count": len(self._outcomes),
            "failed_count": len(failing),
            "rules": [self._rule_payload(outcome) for outcome in self._outcomes],
        }

    def defect_summary(self) -> dict:
        defects = [self._rule_payload(outcome) for outcome in self._failing_outcomes]
        counts = Counter(entry["root_cause"] for entry in defects)
        return {
            "generated_at": self.iso_timestamp,
            "defect_count": len(defects),
            "defects": defects,
            "root_cause_counts": dict(counts),
        }

    def release_decision(self) -> dict:
        failing = self._failing_outcomes
        status = self.status
        decision_reason = self._decision_reason(status, failing)
        payload = {
            "status": status,
            "timestamp": self.iso_timestamp,
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
            if failing:
                return f"Critical failure {failing[0].rule_id} blocks release."
            return "Critical data quality failures block release."
        if status == STATUS_WARNING_ONLY:
            return "Warnings or info-level defects were raised; release may proceed with caution."
        return "All validations passed; release is ready."

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
