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


def _outcome(rule_id: str, severity: str, passed: bool = False) -> RuleOutcome:
    return RuleOutcome(rule_id=rule_id, description="test", severity=severity, passed=passed)


def test_release_report_ready_when_all_rules_pass():
    report = ReleaseReport([], timestamp=FIXED_TIMESTAMP)
    assert report.status == STATUS_READY
    summary = report.validation_summary()
    assert summary["rule_count"] == 0
    assert summary["failed_count"] == 0
    assert summary["rules"] == []
    decision = report.release_decision()
    assert decision["status"] == STATUS_READY
    assert decision["failed_rules"] == []


def test_release_report_warns_on_non_critical_defects():
    warning_rule = _outcome("BR-04", severity="warning")
    report = ReleaseReport([warning_rule], timestamp=FIXED_TIMESTAMP)
    assert report.status == STATUS_WARNING_ONLY
    decision = report.release_decision()
    assert decision["status"] == STATUS_WARNING_ONLY
    assert "blocking_root_causes" not in decision
    defect_summary = report.defect_summary()
    assert defect_summary["defect_count"] == 1
    assert defect_summary["root_cause_counts"]["invalid_business_rule_state"] == 1


def test_release_report_blocks_on_critical_duplicate():
    critical_rule = _outcome("exam_results-uniqueness-1", severity="critical")
    report = ReleaseReport([critical_rule], timestamp=FIXED_TIMESTAMP)
    assert report.status == STATUS_BLOCKED
    decision = report.release_decision()
    assert decision["status"] == STATUS_BLOCKED
    assert decision["blocking_root_causes"] == ["duplicate_record"]
    assert decision["failed_rules"][0]["root_cause"] == "duplicate_record"
    defect_summary = report.defect_summary()
    assert defect_summary["root_cause_counts"] == {"duplicate_record": 1}
