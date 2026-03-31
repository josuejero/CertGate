"""Orchestrates the CertGate CRM integrity pipeline end-to-end."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Sequence

from certgate.analytics import build_weekly_exec_summary, run_ops_insights
from certgate.config import BusinessRuleCallable, PipelineConfig, TablesMapping
from certgate.ingest import load_bundle
from certgate.reporting import ReportWriter, ReleaseReport
from certgate.rules.business import default_crm_business_rules
from certgate.rules.schema import RuleOutcome, apply_schema_definition, get_schema_definition


@dataclass
class BundleRun:
    bundle_name: str
    tables: TablesMapping
    report: ReleaseReport


class ReleaseGatePipeline:
    """Runs schema + business rule validations and writes release reports."""

    def __init__(self, config: PipelineConfig) -> None:
        self.config = config
        self._current_run: BundleRun | None = None

    def _evaluate_bundle(
        self,
        bundle_name: str,
        extra_business_checks: Sequence[BusinessRuleCallable] | None = None,
    ) -> BundleRun:
        tables = load_bundle(
            self.config.data_root,
            bundle_name,
            targets=[cfg.name for cfg in self.config.schema_targets],
        )
        outcomes: list[RuleOutcome] = []
        table_counts: dict[str, int] = {}
        for schema_config in self.config.schema_targets:
            table = tables[schema_config.name]
            definition = get_schema_definition(schema_config.name)
            normalized_df, schema_outcomes = apply_schema_definition(
                df=table.df,
                definition=definition,
                dtype_rule_prefix=schema_config.dtype_rule_prefix,
                uniqueness_rule_prefix=schema_config.uniqueness_rule_prefix,
                null_rule_prefix=schema_config.null_rule_prefix,
            )
            table.df = normalized_df
            table_counts[schema_config.name] = int(len(normalized_df))
            outcomes.extend(schema_outcomes)

        if self.config.business_rules:
            for rule in self.config.business_rules:
                outcomes.extend(rule.rule(tables))
        else:
            outcomes.extend(
                default_crm_business_rules(
                    tables=tables,
                    recent_touch_days=self.config.temporal.recent_touch_days,
                    stale_stage_days=self.config.temporal.stale_stage_days,
                    reference_time=self.config.temporal.reference_time,
                )
            )

        if extra_business_checks:
            for checker in extra_business_checks:
                outcomes.extend(checker(tables))

        report = ReleaseReport(
            outcomes=outcomes,
            bundle_name=bundle_name,
            table_counts=table_counts,
        )
        return BundleRun(bundle_name=bundle_name, tables=tables, report=report)

    def run(
        self,
        bundle_name: str | None = None,
        extra_business_checks: Sequence[BusinessRuleCallable] | None = None,
    ) -> ReleaseReport:
        bundle_key = bundle_name or self.config.bundle
        self._current_run = self._evaluate_bundle(bundle_key, extra_business_checks)
        return self._current_run.report

    def write_reports(self, report_dir: Path | None = None) -> Path:
        if self._current_run is None:
            raise RuntimeError("Pipeline must be run before writing reports.")
        target_dir = Path(report_dir) if report_dir is not None else self.config.reports_dir
        ReportWriter(target_dir).write(self._current_run.report)
        return target_dir

    def write_ops_insights(self, report_dir: Path | None = None) -> dict[str, list[dict]]:
        if self._current_run is None:
            raise RuntimeError("Pipeline must be run before writing ops insights.")
        base_dir = Path(report_dir) if report_dir is not None else self.config.reports_dir
        return run_ops_insights(
            tables=self._current_run.tables,
            sql_dir=self.config.sql_dir,
            output_dir=base_dir / "ops_insights",
        )

    def write_weekly_summary(
        self,
        insights: dict[str, list[dict]] | None = None,
        report_dir: Path | None = None,
        baseline_bundle: str | None = None,
    ) -> dict:
        if self._current_run is None:
            raise RuntimeError("Pipeline must be run before writing the weekly summary.")
        base_dir = Path(report_dir) if report_dir is not None else self.config.reports_dir
        current_insights = insights or self.write_ops_insights(report_dir=base_dir)

        baseline_report = None
        baseline_insights = None
        baseline_key = baseline_bundle
        if baseline_key:
            baseline_run = self._evaluate_bundle(baseline_key)
            baseline_report = baseline_run.report
            baseline_insights = run_ops_insights(
                tables=baseline_run.tables,
                sql_dir=self.config.sql_dir,
                output_dir=base_dir / "ops_insights" / "_baseline",
            )

        return build_weekly_exec_summary(
            report=self._current_run.report,
            insights=current_insights,
            owners_df=self._current_run.tables["owners"].df,
            output_dir=base_dir,
            baseline_report=baseline_report,
            baseline_insights=baseline_insights,
        )

    def write_demo_artifacts(self, report_dir: Path | None = None) -> Path:
        base_dir = Path(report_dir) if report_dir is not None else self.config.reports_dir
        demo_dir = base_dir / "demo"
        before_run = self._evaluate_bundle(self.config.demo.before_bundle)
        after_run = self._evaluate_bundle(self.config.demo.after_bundle)
        ReportWriter(demo_dir / "before").write(before_run.report)
        ReportWriter(demo_dir / "after").write(after_run.report)
        return demo_dir

    def generate_all_outputs(
        self,
        bundle_name: str | None = None,
        extra_business_checks: Sequence[BusinessRuleCallable] | None = None,
    ) -> ReleaseReport:
        report = self.run(bundle_name=bundle_name, extra_business_checks=extra_business_checks)
        self.write_reports()
        insights = self.write_ops_insights()
        baseline_bundle = (
            self.config.demo.before_bundle
            if self._current_run and self._current_run.bundle_name == self.config.demo.after_bundle
            else None
        )
        self.write_weekly_summary(insights=insights, baseline_bundle=baseline_bundle)
        self.write_demo_artifacts()
        return report

    @property
    def report(self) -> ReleaseReport | None:
        return self._current_run.report if self._current_run else None

    @property
    def tables(self) -> TablesMapping | None:
        return self._current_run.tables if self._current_run else None


__all__ = ["BundleRun", "ReleaseGatePipeline"]
