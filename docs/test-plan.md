# CertGate CRM Test Plan

The test suite preserves the repo’s layered structure while switching the domain to CRM integrity.

## Layers

| Layer | Focus |
| --- | --- |
| Smoke | Repo health, CRM schema availability, dataset discovery, GX suite presence |
| Unit | Schema coercion, dedupe logic, activity/owner/account integrity, stale-touch/stage logic, report serialization |
| Functional | Good-bundle readiness, full artifact generation, Great Expectations checkpoint execution on supported interpreters |
| Regression | Targeted CRM failure bundles for duplicate emails, duplicate domains, broken joins, inactive owner routing, missing next steps, stale stages, and domain mismatches |
| UAT | Before/after demo artifact expectations (`Blocked` before cleanup, `Ready` after cleanup) |
| Defect triage | Root-cause mix across duplicate, integrity, and business-rule failures |

## Acceptance Criteria

- `data/good` produces `Ready`
- `data/bad/demo-before-cleanup` produces `Blocked`
- Regression bundles trigger the intended CRM rule IDs
- Root artifacts, demo comparison artifacts, weekly summary, and ops insights are written on a standard pipeline run
- Great Expectations Data Docs can be regenerated on Python 3.10-3.13
