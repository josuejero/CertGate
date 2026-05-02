from importlib.util import find_spec
from pathlib import Path
import subprocess
import sys

import pytest

from certgate.config import PipelineConfig
from certgate.pipeline import ReleaseGatePipeline
from certgate.reporting import STATUS_READY
from certgate.rules.schema import SCHEMA_TARGETS, apply_schema_definition, get_schema_definition


pytestmark = pytest.mark.functional


@pytest.mark.parametrize("schema_name", SCHEMA_TARGETS)
def test_schema_validation_passes_for_canonical_data(schema_name, good_bundle):
    table = good_bundle[schema_name]
    _, outcomes = apply_schema_definition(table.df, get_schema_definition(schema_name))
    assert not [
        outcome for outcome in outcomes if outcome.severity == "critical" and not outcome.passed
    ]


def test_pipeline_status_for_good_bundle(tmp_path):
    repo_root = Path(__file__).resolve().parents[2]
    pipeline = ReleaseGatePipeline(
        PipelineConfig(
            data_root=repo_root / "data",
            bundle="good",
            reports_dir=tmp_path / "reports",
            sql_dir=repo_root / "sql",
        )
    )
    report = pipeline.generate_all_outputs()
    assert report.status == STATUS_READY
    assert (tmp_path / "reports" / "ops_insights" / "conversion_snapshot.json").exists()
    assert (tmp_path / "reports" / "weekly_exec_summary.md").exists()


def test_great_expectations_checkpoint_runs_for_crm_bundle():
    if find_spec("great_expectations") is None:
        pytest.skip("great_expectations is not installed in the local test environment")
    if sys.version_info[:2] > (3, 13):
        pytest.skip("Great Expectations is intentionally blocked on Python 3.14+")

    repo_root = Path(__file__).resolve().parents[2]
    subprocess.run(
        [sys.executable, "scripts/run_great_expectations.py"],
        cwd=repo_root,
        check=True,
    )
    assert (repo_root / "gx" / "data_docs" / "local_site" / "index.html").exists()
