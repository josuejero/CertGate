from pathlib import Path

from certgate.config import PipelineConfig, TablesMapping
from certgate.pipeline import ReleaseGatePipeline
from certgate.reporting import STATUS_BLOCKED
from certgate.rules.schema import RuleOutcome


def test_pipeline_blocks_on_canonical_bundle(tmp_path: Path) -> None:
    pipeline = ReleaseGatePipeline(PipelineConfig())
    report = pipeline.run()
    assert report.status == STATUS_BLOCKED
    assert report.defect_summary()["defect_count"] == 5
    report.write_reports(tmp_path)
    assert (tmp_path / "validation_summary.json").exists()


def test_pipeline_honors_extra_business_checks() -> None:
    def failing_rule(_: TablesMapping) -> list[RuleOutcome]:
        return [
            RuleOutcome(
                rule_id="EXTRA-01",
                description="Extra validation",
                passed=False,
                severity="warning",
            )
        ]

    pipeline = ReleaseGatePipeline(PipelineConfig())
    report = pipeline.run(extra_business_checks=[failing_rule])
    assert any(rule["rule_id"] == "EXTRA-01" for rule in report.validation_summary()["rules"])
