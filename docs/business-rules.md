# CertGate Business Rules

## Purpose
This document defines the data contract for CertGate's inbound feeds so that expectations can be translated into Great Expectations suites, validation artifacts, and release-gate decisions before any code is run. The focus is on the candidate master list, examination results, and certification status streams, covering schema, primary/uniqueness keys, valid ranges/windows, certification mappings, cross-file joins, freshness expectations, and severity tagging.

## Files and Schemas
| File | Required columns | Primary / uniqueness key | Notes on completeness/uniqueness |
| --- | --- | --- | --- |
| `candidates.csv` | `candidate_id`, `birth_date`, `email`, `first_name`, `last_name`, `created_ts`, `source_system` | `candidate_id` is the primary key; `email` must be globally unique across records | Candidate records must be complete and unique. Missing `candidate_id` or duplicate `email` should halt validation (critical). |
| `exam_results.csv` | `exam_result_id`, `candidate_id`, `exam_code`, `exam_date`, `attempt_number`, `score`, `score_scaled`, `pass_fail`, `exam_location`, `file_received_ts` | Compound uniqueness on (`candidate_id`, `exam_code`, `attempt_number`) enforces one row per attempt; `exam_result_id` is also a unique identifier for traceability | `score` and `score_scaled` must be numeric; missing combination keys or duplicates trigger critical severity. |
| `certification_status.csv` | `cert_status_id`, `candidate_id`, `certification_level`, `certified_ts`, `status_source`, `status_effective_ts` | `cert_status_id` primary key; `candidate_id` must be unique per `certification_level` at a point in time (no overlapping records for the same level) | Certification rows describe the current status; duplicate `candidate_id` + `certification_level` with overlapping timestamps breachs consistency and uniqueness expectations (critical). |

## Allowed Score Range
- `score` and `score_scaled` must fall within 0–100 inclusive (percentage). Values outside this range indicate data corruption/entry errors and are tagged as **warning** severity when caught in validation.
- Non-numeric or null scores mean the entire row is invalid and should be routed to the `bad` synthetic bucket for regression testing.

## Valid Exam-Date Window
- All `exam_date` values must fall within the 12-month window ending on the validation execution date (e.g., if validations run on March 2, 2026, allowed window is March 2, 2025 through March 2, 2026). Dates outside this window are flagged as **warning** since they indicate stale or future-dated records.
- The exam feed may include a `file_received_ts`; this timestamp must not be older than 24 hours past the `exam_date` to satisfy timeliness expectations.

## Pass/Fail to Certification-Status Mapping
| Pass/Fail value | Expected certification status | Severity if mismatch |
| --- | --- | --- |
| `Pass`, `pass`, `P` | `Certified`, `Certification Granted` | **Critical** if certification status is blank or not in the allowed list for a passing exam (reports block release). |
| `Fail`, `fail`, `F` | `NeedsReview`, `Certification Pending`, `Not Certified` | **Warning** if a failing attempt is associated with a `Certified` status; this might warrant manual review. |

Certification rows without a corresponding attempt (no pass/fail link) are allowed but should be noted as **info** (for audit tracing) unless they contradict historical results.

## Cross-File Candidate ID Matching
- Every `candidate_id` in `exam_results.csv` and `certification_status.csv` must exist in `candidates.csv`. Missing candidate references break **consistency** and are tagged as **critical**.
- Candidate records that appear in the master after an exam entry should trigger a **warning** until the master is refreshed; this ensures near-real-time consistency between upstream sources.

## Freshness Thresholds for Inbound Files
| File | Freshness definition | Allowed lag | Severity |
| --- | --- | --- | --- |
| `candidates.csv` | `created_ts` top-of-file or file metadata | Ingested within 24 hours of the most recent `created_ts` | **Warning** (if gap > 24h) / **Critical** (> 72h or running behind multiple days) |
| `exam_results.csv` | `file_received_ts` or max `exam_date`; also expect `file_received_ts` in metadata | Ingested within 24 hours of the latest `exam_date` | **Critical** if lag > 24h (production gate); **Warning** if lag 24–48h to prompt action. |
| `certification_status.csv` | `status_effective_ts` | Same-day delivery preferred; maximum 48h lag | **Warning** if lag 24–48h, **Info** if on time, **Critical** if > 48h during release readiness. |

## Severity per Rule
The rule severity follows the data-quality dimension impacted:
- **Completeness/Uniqueness (critical)**: missing required columns, duplicate primary keys, and candidate_id duplicates.
- **Validity (warning)**: out-of-range scores, invalid pass/fail to certification mappings, invalid datetimes (exam_date, certified_ts).
- **Consistency (critical/warning)**: missing candidate master joins (critical) or mismatched status (warning).
- **Timeliness (warning/critical)**: stale files or CPT (candidate processing time) beyond thresholds.
- **Info**: metadata confirmations (e.g., optional `source_system`) that do not block releases but provide context.

This business rule sheet will guide expectation suite authoring, synthetic data creation, and automated test coverage before code is written.
