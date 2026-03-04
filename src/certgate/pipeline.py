"""Orchestrates the CertGate validation pipeline end-to-end."""

from __future__ import annotations

from pathlib import Path
from typing import Sequence

from certgate.config import (
    BusinessRuleCallable,
    BusinessRuleConfig,
    FreshnessConfig,
    PipelineConfig,
    TablesMapping,
)
from certgate.ingest import load_bundle
from certgate.reporting import ReportWriter, ReleaseReport
from certgate.rules.business import (
    check_cert_candidate_fk,
    check_exam_candidate_fk,
    check_pass_fail_certification,
    evaluate_table_freshness,
)
from certgate.rules.schema import RuleOutcome, apply_schema_definition, get_schema_definition


def _exam_candidate_fk_rule(tables: TablesMapping) -> Sequence[RuleOutcome]:
    return [
        check_exam_candidate_fk(
            tables["exam_results"].df,
            tables["candidates"].df,
        )
    ]


def _cert_candidate_fk_rule(tables: TablesMapping) -> Sequence[RuleOutcome]:
    return [
        check_cert_candidate_fk(
            tables["certification_status"].df,
            tables["candidates"].df,
        )
    ]


def _pass_fail_rule(tables: TablesMapping) -> Sequence[RuleOutcome]:
    return check_pass_fail_certification(
        tables["exam_results"].df,
        tables["certification_status"].df,
    )


def _freshness_rule_factory(freshness: FreshnessConfig) -> BusinessRuleCallable:
    def _freshness_rule(tables: TablesMapping) -> Sequence[RuleOutcome]:
        return [
            evaluate_table_freshness(
                table=tables["exam_results"],
                timestamp_column=freshness.timestamp_column,
                metadata_timestamp_keys=freshness.metadata_timestamp_keys,
                max_allowed_hours=freshness.max_allowed_hours,
                warning_hours=freshness.warning_hours,
                rule_id=freshness.rule_id,
            )
        ]

    return _freshness_rule


def _default_business_rules(freshness: FreshnessConfig) -> Sequence[BusinessRuleConfig]:
    return (
        BusinessRuleConfig(name="exam_candidate_fk", rule=_exam_candidate_fk_rule),
        BusinessRuleConfig(name="cert_candidate_fk", rule=_cert_candidate_fk_rule),
        BusinessRuleConfig(name="pass_fail_cert_mapping", rule=_pass_fail_rule),
        BusinessRuleConfig(name="exam_freshness", rule=_freshness_rule_factory(freshness)),
    )


class ReleaseGatePipeline:
    """Runs schema + business rule validations and writes release reports."""

    def __init__(self, config: PipelineConfig) -> None:
        self.config = config
        self._report: ReleaseReport | None = None

    def run(
        self,
        bundle_name: str | None = None,
        extra_business_checks: Sequence[BusinessRuleCallable] | None = None,
    ) -> ReleaseReport:
        bundle_key = bundle_name or self.config.bundle
        tables = load_bundle(
            self.config.data_root,
            bundle_key,
            targets=[cfg.name for cfg in self.config.schema_targets],
        )
        outcomes: list[RuleOutcome] = []
        for schema_config in self.config.schema_targets:
            table = tables[schema_config.name]
            definition = get_schema_definition(schema_config.name)
            _, schema_outcomes = apply_schema_definition(
                df=table.df,
                definition=definition,
                dtype_rule_prefix=schema_config.dtype_rule_prefix,
                uniqueness_rule_prefix=schema_config.uniqueness_rule_prefix,
            )
            outcomes.extend(schema_outcomes)

        business_rules = (
            self.config.business_rules
            if self.config.business_rules
            else _default_business_rules(self.config.freshness)
        )
        for rule in business_rules:
            outcomes.extend(rule.rule(tables))

        if extra_business_checks:
            for checker in extra_business_checks:
                outcomes.extend(checker(tables))

        self._report = ReleaseReport(outcomes)
        return self._report

    def write_reports(self, report_dir: Path | None = None) -> Path:
        if self._report is None:
            raise RuntimeError("Pipeline must be run before writing reports.")
        target_dir = Path(report_dir) if report_dir is not None else self.config.reports_dir
        ReportWriter(target_dir).write(self._report)
        return target_dir

    @property
    def report(self) -> ReleaseReport | None:
        return self._report


__all__ = ["ReleaseGatePipeline"]
