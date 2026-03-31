WITH context AS (
  SELECT bundle_name, CAST(reference_time AS TIMESTAMPTZ) AS reference_time
  FROM bundle_context
),
duplicate_domains AS (
  SELECT company_domain
  FROM accounts
  GROUP BY company_domain
  HAVING COUNT(*) > 1
),
stale_opportunities AS (
  SELECT opportunity_id
  FROM opportunities, context
  WHERE stage NOT IN ('Closed Won', 'Closed Lost')
    AND (
      last_stage_change_at IS NULL
      OR CAST(last_stage_change_at AS TIMESTAMPTZ) < context.reference_time - INTERVAL 21 DAY
    )
),
missing_next_step AS (
  SELECT opportunity_id
  FROM opportunities
  WHERE stage NOT IN ('Closed Won', 'Closed Lost')
    AND COALESCE(trim(next_step), '') = ''
),
inactive_owner_mappings AS (
  SELECT opportunities.opportunity_id
  FROM opportunities
  JOIN owners ON owners.owner_id = opportunities.owner_id
  WHERE opportunities.stage NOT IN ('Closed Won', 'Closed Lost')
    AND owners.is_active = FALSE
)
SELECT
  context.bundle_name,
  (SELECT COUNT(*) FROM leads)
    + (SELECT COUNT(*) FROM accounts)
    + (SELECT COUNT(*) FROM opportunities)
    + (SELECT COUNT(*) FROM activities)
    + (SELECT COUNT(*) FROM owners) AS records_scanned,
  (SELECT COUNT(*) FROM duplicate_domains) AS duplicate_domains,
  (SELECT COUNT(*) FROM stale_opportunities) AS stale_opportunities,
  (SELECT COUNT(*) FROM missing_next_step) AS open_opportunities_with_no_next_step,
  (SELECT COUNT(*) FROM inactive_owner_mappings) AS inactive_owner_mappings
FROM context;
