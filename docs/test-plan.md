# CertGate QA Test Plan

## Overview
The QA testing plan verifies the business rules defined for CertGate using Great Expectations (GX) suites, pytest layers, synthetic datasets, and automated reporting. Each rule maps to one or more GX expectations housed under `gx/expectations/`, exercised by pytest (unit, functional, regression, and UAT layers), and surfaced via CI artifacts (`reports/validation_summary.json`, `reports/release_decision.json`, `reports/junit.xml`). The plan also references the defect log (`docs/defect-log.md`) so every gating failure is traceable.

## Rule-to-Test Traceability
| Business rule | GX expectation candidates | Pytest layer | Synthetic data role | Acceptance criteria |
| --- | --- | --- | --- | --- |
| Candidate master schema & uniqueness | `expect_table_columns_to_match_ordered_list`, `expect_column_values_to_not_be_null(candidate_id, email)`, `expect_column_values_to_be_unique(candidate_id)`, `expect_column_values_to_be_unique(email)` | Unit + Functional | Provide `good/candidates.csv` and `bad/candidates.csv` for missing columns or duplicate emails | GX suite passes when schema is complete/unique; failing data lands in `bad` set and pytest marks suite as failed, producing JUnit + validation summary. |
| Exam results schema, composite key, score range, pass/fail mapping, exam_date window | `expect_column_values_to_be_between(score, 0, 100)`, `expect_column_values_to_be_in_set(pass_fail, ['Pass','Fail','P','F'])`, `expect_column_pair_values_A_to_be_B(candidate_id, exam_code, [rows unique])`, `expect_column_values_to_be_between(exam_date, (today-12mo), today)` | Functional + Regression | `good/exam_results.csv` (valid scores, valid pass/fail), `bad/exam_results.csv` (duplicate attempt, score 123) | Suite produces validation results; regression tests assert that `bad` data triggers the expected expectation names and severity. |
| Certification status validity and mapping | `expect_column_values_to_be_in_set(certification_level, ['Associate','Professional','Expert'])`, `expect_column_values_to_be_in_set(status_source, [...])`, `expect_column_values_to_be_unique(candidate_id, condition=level)` | Regression | Synthetic `certification_status.csv` with mismatched statuses (pass + Not Certified) | On validation failure, defect log entry templates are populated and the release decision artifact is set to `investigate`. |
| Cross-file candidate matching | `expect_column_values_to_be_in_set(candidate_id in exam_results, column=candidates.candidate_id)`, same for certification_status | Functional + UAT | Regression data where an exam_result points to a missing candidate | GX suite flagged; pytest regression captures severity to differentiate info/warning/critical statuses. |
| Freshness & file lag | Custom expectation `expect_table_column_values_to_be_no_more_than_n_days_behind(column=file_received_ts, max_lag=1)` or metadata check plus `expect_column_values_to_be_between` using metadata references | UAT + CI smoke | Synthetic metadata to simulate 25-hour lag for exam file | CI gate ensures `reports/release_decision.json` toggles to `block` when lag expectation fails and signals the severity based on the rule matrix. |

## GX Expectation Suites and Structure
1. **Candidate Suite** (`gx/expectations/candidates_suite.yml`): covers schema, completeness, uniqueness, and optional metadata. Run during every dry run (pytest unit/functional) to prevent corrupt master loads.
2. **Exam Results Suite** (`gx/expectations/exam_results_suite.yml`): covers composite keys, scores, pass/fail mapping, exam_date window, and candidate joinable references; executed in regression and UAT layers with synthetic failing pushes.
3. **Certification Suite** (`gx/expectations/cert_status_suite.yml`): ensures status mapping integrity, uniqueness per level, and date validity; invoked in functional/regression contexts and again in UAT with near-production data.
4. **Freshness Suite** (`gx/expectations/freshness_suite.yml`): validates metadata timestamps (ingest time vs. exam_date) and ensures each file's lag does not exceed defined thresholds; targeted by nightly CI jobs for timeliness assurance.

Each suite produces `gx/validation_results/` that GitHub Actions archives and surfaces via GitHub Pages (per `gx/data_docs/`). Suites should also emit structured summaries to `reports/validation_summary.json` for downstream release automation.

## Pytest Layers
- **Unit**: quick checks on standalone rule functions (e.g., verifying helper methods for score validation). Use parametrized fixtures referencing `src/rules/` functions and rely on synthetic good/bad CSV snippets stored under `data/`. These tests run locally and in every PR pipeline, generating `reports/junit.xml`.
- **Functional**: integrate CSV ingestion → GX expectation suite w/ `gx/expectations/*.yml`. Validate `candidates` feed by running the expectation suite against both `good` and `bad` data, verifying JUnit outputs and summarizing severity.
- **Regression**: include more complex scenarios (duplicate candidates, pass/fail mismatches, metadata lag). Tests assert that failure metadata (rule ID, severity, message) matches the rule severity matrix and that the release decision artifact reflects the correct gating action (`block`, `warn`, `allow-with-notes`).
- **UAT**: simulate near-production ingestion (daily pipeline with all three files) run by a pre-release job; ensures GX data docs are generated and defects are logged to `docs/defect-log.md` with context for each failing expectation.

## Synthetic Dataset Usage
- `data/good/` contains canonical CSVs for each file with valid field values. These drive happy-path regression tests and serve as reference for GX expectation authors.
- `data/bad/` captures targeted failures per rule (missing columns, duplicate candidate IDs, stale timestamps). Their expected fail statuses are recorded in `docs/defect-log.md` entries for reproducibility.
- `data/regression/` merges multi-file scenarios (e.g., candidate plus exam plus certification) to exercise cross-file checks.

## CI Automation & Reporting
- GitHub Actions workflow (`.github/workflows/ci.yml`) runs all pytest layers, executes GX validation suites, collects reports, and uploads validation docs to GitHub Pages via a dedicated job.
- Post-test steps convert GX validation results to `reports/validation_summary.json` (including rule ID, pass/fail, severity, remediation link) and `reports/release_decision.json` (gate outcome: `block`, `warn`, `pass`). If Allure is enabled, `reports/allure-results/` gets populated and published as an optional artifact.
- Failures automatically append to `docs/defect-log.md` with timestamp, rule ID, severity, impacted file, and remediation steps so data teams can triage quickly.

### Release gating exports
Post-test automation now serializes every evaluated `RuleOutcome` into three artifacts:

| File | Purpose |
| --- | --- |
| `reports/validation_summary.json` | Lists every rule with its pass/fail flag, severity, remediation link (pointing at `docs/rule-severity-matrix.md`), and inferred root-cause tag so teams can quickly trace issues. |
| `reports/release_decision.json` | Captures the gate status (`Ready`, `Warning only`, or `Blocked`), a short decision reason, and the failing rule list (critical failures also surface their `blocking_root_causes`). |
| `reports/defect_summary.json` | Contains each defect (i.e., failing rule) with exactly one root cause from `{schema_issue, integrity_issue, stale_data, duplicate_record, invalid_business_rule_state}` plus a root cause tally for dashboards. |

Gate logic follows this priority: any critical failure (schema, integrity, or freshness) triggers `Blocked`, the presence of only non-critical issues yields `Warning only`, and pristine runs produce `Ready`. Structured summaries keep GX checkpoints and CI consumers aligned with production-aware release decisions.

## Acceptance Criteria
1. Every business rule in `docs/business-rules.md` has a corresponding expectation or pytest guard described in this plan.
2. GX validation suites run cleanly against `data/good`, and `data/bad` data yields predictable failures, as documented in the defect log.
3. Reports (`validation_summary.json`, `release_decision.json`, `junit.xml`) capture severity details and link back to the rule severity matrix; CI or manual gating decisions reference these artifacts.
4. Rule failures that differ in severity update `docs/rule-severity-matrix.md` with actionable remediation steps and expected follow-up (e.g., ticket creation, notification). 5. QA layers are traceable: rule → GX expectation → pytest test → report involving explicit severity (critical/warning/info).
