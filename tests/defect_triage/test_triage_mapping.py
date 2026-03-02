from pathlib import Path

import pytest

from src.rules.business_rules import check_pass_fail_certification


def load_severity_matrix() -> dict[str, dict[str, str]]:
    matrix_path = Path(__file__).resolve().parents[2] / "docs" / "rule-severity-matrix.md"
    lines = matrix_path.read_text().splitlines()
    table_lines = [line for line in lines if line.strip().startswith("|")]
    if len(table_lines) < 3:
        raise RuntimeError("Rule severity matrix table is missing or malformed")

    header_cells = [cell.strip() for cell in table_lines[0].strip("|").split("|")]
    data_lines = table_lines[2:]
    matrix: dict[str, dict[str, str]] = {}
    for line in data_lines:
        if not line.strip() or line.strip().startswith("| ---"):
            continue
        cells = [cell.strip() for cell in line.strip("|").split("|")]
        if len(cells) != len(header_cells):
            continue
        row = dict(zip(header_cells, cells))
        matrix[row["Rule ID"]] = row
    return matrix


REPORTING_BAD_BUNDLE = (
    Path(__file__).resolve().parents[2]
    / "data"
    / "bad"
    / "cert-status-inconsistent-with-exam"
)


pytestmark = pytest.mark.defect_triage


@pytest.mark.parametrize(
    "dataset_bundle",
    [
        pytest.param(REPORTING_BAD_BUNDLE, id="cert-status-inconsistent"),
    ],
    indirect=["dataset_bundle"],
)
def test_cert_status_failure_maps_to_consistency_dimension(dataset_bundle):
    matrix = load_severity_matrix()
    exam_df = dataset_bundle["exam_results"].df
    cert_df = dataset_bundle["certification_status"].df
    outcomes = check_pass_fail_certification(exam_df, cert_df)
    failing = [outcome for outcome in outcomes if not outcome.passed]
    assert failing
    segments = failing[0].rule_id.split("-")
    root_id = "-".join(segments[:2]) if len(segments) > 1 else segments[0]
    assert root_id in matrix
    assert matrix[root_id]["Data Quality Dimension"] == "Consistency"
