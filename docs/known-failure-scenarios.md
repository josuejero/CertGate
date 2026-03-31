# Known Failure Scenarios

CertGate ships targeted CRM regression bundles under `data/regression` so reviewers can inspect how the gate responds to realistic data-quality failures.

| Bundle | Story |
| --- | --- |
| `duplicate-lead-email` | Contact dedupe failure on the primary email anchor |
| `duplicate-account-domain` | Company dedupe failure on the primary domain anchor |
| `email-multiple-accounts` | Same person linked to multiple accounts |
| `missing-owner-reference` | Broken owner/account/activity joins |
| `inactive-owner-open-opportunity` | Open pipeline still assigned to an inactive rep |
| `missing-employee-count-enterprise` | Missing firmographics on an enterprise account |
| `missing-next-step` | Open deal hygiene gap |
| `stale-opportunity-stage` | Stalled opportunity age |
| `lead-account-domain-mismatch` | Lead/company linkage mismatch |
