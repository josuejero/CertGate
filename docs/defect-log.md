# CertGate CRM Defect Log

This log captures the canonical failure patterns represented in synthetic bundles.

| Scenario | Key Rules Triggered | Notes |
| --- | --- | --- |
| Demo before cleanup | Duplicate email/domain, broken owner/object references, inactive owner, missing next step, stale follow-up | Used for recruiter-facing before/after comparison |
| Duplicate lead email | `leads-uniqueness-unique-2` | Verifies email dedupe anchor |
| Duplicate account domain | `accounts-uniqueness-unique-2` | Verifies company-domain dedupe anchor |
| Email mapped to multiple accounts | `crm-email-multiple-accounts` | Simulates bad CRM linking after imports |
| Missing references | `crm-lead-owner-fk`, `crm-opportunity-account-fk`, `crm-activity-object-reference` | Covers orphan owner/account/activity joins |
| Inactive owner on open opportunity | `crm-inactive-owner-open-opportunity` | Simulates routing drift during territory changes |
| Missing enterprise employee count | `crm-enterprise-employee-count` | Firmographic completeness warning |
| Missing next step | `crm-open-opportunity-next-step` | Deal hygiene warning |
| Stale opportunity stage | `crm-stale-opportunity-stage` | Stage age warning |
| Lead/account domain mismatch | `crm-lead-account-domain-mismatch` | Linkage consistency warning |
