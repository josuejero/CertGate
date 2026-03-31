# CertGate CRM Business Rules

CertGate now validates CRM/revops bundles instead of certification feeds. The data contract covers `leads`, `accounts`, `opportunities`, `activities`, and `owners`, with rule outcomes written into release artifacts and surfaced on the static demo.

## Entity Rules

| Entity | Primary checks |
| --- | --- |
| `leads.csv` | Required columns, non-null `lead_id`/`email`, unique email, valid lifecycle stage, recent touch for `MQL`/`SQL`/`Opportunity`, linked account integrity |
| `accounts.csv` | Required columns, non-null `account_id`/`company_domain`, unique domain, numeric `employee_count`, enterprise firmographic completeness |
| `opportunities.csv` | Required columns, non-null identifiers, unique `opportunity_id`, valid stage, positive amount for `Closed Won`, close-date chronology, active-owner routing, next-step presence, stale stage checks |
| `activities.csv` | Required columns, non-null identifiers, unique `activity_id`, valid `object_type`, valid object reference, valid owner mapping |
| `owners.csv` | Required columns, non-null `owner_id`/`owner_email`, unique identifiers, parseable boolean `is_active` |

## Blocking Integrity Rules

- Duplicate lead email
- Duplicate account domain
- Same email mapped to multiple accounts
- Missing owner references on leads/accounts/opportunities/activities
- Missing account references on linked leads and opportunities
- Invalid activity object type/object ID
- `Closed Won` with null or non-positive amount
- `close_date < created_at`
- Inactive owner on an open opportunity

## Warning Rules

- Same company domain routed to multiple reps
- Missing `employee_count` on enterprise accounts
- Missing `next_step` on open opportunities
- Missing recent touch on active leads or open opportunities
- Stale open opportunity stage age (>21 days)
- Lead/account domain mismatch

## Ordered Value Sets

- Lead lifecycle: `Subscriber` -> `Lead` -> `MQL` -> `SQL` -> `Opportunity` -> `Customer`
- Opportunity stage: `Discovery` -> `Qualification` -> `Proposal` -> `Negotiation` -> `Closed Won` / `Closed Lost`

## Temporal Rule Basis

Temporal rules do not use the current clock by default. CertGate derives a bundle reference timestamp from the newest observed activity/stage timestamps in the loaded bundle so demos and regression suites remain stable.
