from pathlib import Path

from certgate.config import PipelineConfig, TablesMapping
from certgate.pipeline import ReleaseGatePipeline
from certgate.reporting import STATUS_READY, STATUS_WARNING_ONLY
from certgate.rules.schema import RuleOutcome


def test_pipeline_ready_for_canonical_bundle(tmp_path: Path) -> None:
    repo_root = Path(__file__).resolve().parents[2]
    pipeline = ReleaseGatePipeline(
        PipelineConfig(
            data_root=repo_root / "data",
            reports_dir=tmp_path,
            sql_dir=repo_root / "sql",
        )
    )
    report = pipeline.generate_all_outputs()
    assert report.status == STATUS_READY
    assert (tmp_path / "validation_summary.json").exists()
    assert (tmp_path / "ops_insights" / "owner_workload.json").exists()
    assert (tmp_path / "weekly_exec_summary.json").exists()


def test_pipeline_honors_extra_business_checks(tmp_path: Path) -> None:
    repo_root = Path(__file__).resolve().parents[2]

    def failing_rule(_: TablesMapping) -> list[RuleOutcome]:
        return [
            RuleOutcome(
                rule_id="EXTRA-01",
                description="Extra validation",
                passed=False,
                severity="warning",
            )
        ]

    pipeline = ReleaseGatePipeline(
        PipelineConfig(
            data_root=repo_root / "data",
            reports_dir=tmp_path,
            sql_dir=repo_root / "sql",
        )
    )
    report = pipeline.run(extra_business_checks=[failing_rule])
    assert report.status == STATUS_WARNING_ONLY
    assert any(rule["rule_id"] == "EXTRA-01" for rule in report.validation_summary()["rules"])
