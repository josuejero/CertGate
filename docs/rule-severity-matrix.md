# CertGate CRM Rule Severity Matrix

| Rule ID / Pattern | Meaning | Severity When Failing | Typical Remediation |
| --- | --- | --- | --- |
| `*-required-columns` | Required CRM fields missing | Critical | Rebuild export with the expected column set before sync |
| `*-nulls-*` | Null/blank primary identifiers | Critical | Restore source IDs or reject malformed rows |
| `*-dtype-*` | Invalid datetime/numeric/boolean coercion | Critical | Correct source typing before ingest |
| `leads-uniqueness-unique-2` | Duplicate lead email | Critical | Deduplicate contacts by email and rerun |
| `accounts-uniqueness-unique-2` | Duplicate account/company domain | Critical | Merge or remove duplicate account rows |
| `crm-email-multiple-accounts` | Same email linked to multiple accounts | Critical | Correct account linkage and reroute ownership |
| `crm-*-owner-fk` | Missing owner reference | Critical | Backfill `owner_id` from the CRM owner table |
| `crm-opportunity-account-fk` / `crm-lead-account-fk` | Missing account reference | Critical | Restore valid `account_id` before sync |
| `crm-activity-object-reference` | Activity points to missing CRM object | Critical | Fix object routing or drop orphaned activity rows |
| `crm-closed-won-amount` | Closed won deal has null/zero amount | Critical | Repair revenue amount before reporting |
| `crm-close-date-chronology` | Close date precedes created date | Critical | Correct source chronology fields |
| `crm-inactive-owner-open-opportunity` | Open deal assigned to inactive owner | Critical | Reassign the opportunity to an active owner |
| `crm-domain-multiple-reps` | Same domain routed to multiple reps | Warning | Review ownership routing rules |
| `crm-enterprise-employee-count` | Missing enterprise employee count | Warning | Backfill firmographics for enterprise accounts |
| `crm-open-opportunity-next-step` | Open opportunity missing next step | Warning | Ask owner to update deal hygiene |
| `crm-lead-recent-touch` / `crm-opportunity-recent-touch` | Missing recent touch | Warning | Trigger follow-up or close stale work |
| `crm-stale-opportunity-stage` | Open opportunity stalled in stage | Warning | Refresh stage or close the deal |
| `crm-lead-account-domain-mismatch` | Linked lead and account domains disagree | Warning | Correct linkage or normalize domain values |
