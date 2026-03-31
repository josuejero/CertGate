"""Unit tests for release reporting helpers."""

from datetime import datetime, timezone

from certgate.reporting import (
    ReleaseReport,
    STATUS_BLOCKED,
    STATUS_READY,
    STATUS_WARNING_ONLY,
)
from certgate.rules.schema import RuleOutcome

FIXED_TIMESTAMP = datetime(2026, 3, 2, 12, 0, tzinfo=timezone.utc)


def _outcome(
    rule_id: str,
    severity: str,
    passed: bool = False,
    details: dict | None = None,
) -> RuleOutcome:
    return RuleOutcome(
        rule_id=rule_id,
        description="test",
        severity=severity,
        passed=passed,
        details=details or {},
    )


def test_release_report_ready_when_all_rules_pass():
    report = ReleaseReport([], bundle_name="good", table_counts={"leads": 2}, timestamp=FIXED_TIMESTAMP)
    assert report.status == STATUS_READY
    summary = report.validation_summary()
    assert summary["rule_count"] == 0
    assert summary["failed_count"] == 0
    assert summary["records_scanned"] == 2
    decision = report.release_decision()
    assert decision["status"] == STATUS_READY
    assert decision["failed_rules"] == []


def test_release_report_warns_on_non_critical_defects():
    warning_rule = _outcome("crm-open-opportunity-next-step", severity="warning")
    report = ReleaseReport([warning_rule], bundle_name="good", timestamp=FIXED_TIMESTAMP)
    assert report.status == STATUS_WARNING_ONLY
    decision = report.release_decision()
    assert decision["status"] == STATUS_WARNING_ONLY
    assert "blocking_root_causes" not in decision


def test_release_report_blocks_on_first_critical_reason():
    warning_rule = _outcome("crm-open-opportunity-next-step", severity="warning")
    critical_rule = _outcome(
        "crm-lead-owner-fk",
        severity="critical",
        details={"owner_issue_counts": {"Owner One": 2}},
    )
    report = ReleaseReport(
        [warning_rule, critical_rule],
        bundle_name="bad/demo-before-cleanup",
        timestamp=FIXED_TIMESTAMP,
    )
    assert report.status == STATUS_BLOCKED
    decision = report.release_decision()
    assert decision["status"] == STATUS_BLOCKED
    assert decision["decision_reason"] == "Critical failure crm-lead-owner-fk blocks CRM sync."
    assert report.top_affected_owners == [{"name": "Owner One", "count": 2}]
