WITH lead_counts AS (
  SELECT owner_id, COUNT(*) AS leads_owned
  FROM leads
  GROUP BY owner_id
),
account_counts AS (
  SELECT owner_id, COUNT(*) AS accounts_owned
  FROM accounts
  GROUP BY owner_id
),
open_opportunity_counts AS (
  SELECT owner_id, COUNT(*) AS open_opportunities, COALESCE(SUM(amount), 0) AS open_pipeline_amount
  FROM opportunities
  WHERE stage NOT IN ('Closed Won', 'Closed Lost')
  GROUP BY owner_id
)
SELECT
  owners.owner_id,
  owners.owner_name,
  owners.team,
  owners.manager,
  owners.is_active,
  COALESCE(lead_counts.leads_owned, 0) AS leads_owned,
  COALESCE(account_counts.accounts_owned, 0) AS accounts_owned,
  COALESCE(open_opportunity_counts.open_opportunities, 0) AS open_opportunities,
  COALESCE(open_opportunity_counts.open_pipeline_amount, 0) AS open_pipeline_amount
FROM owners
LEFT JOIN lead_counts ON lead_counts.owner_id = owners.owner_id
LEFT JOIN account_counts ON account_counts.owner_id = owners.owner_id
LEFT JOIN open_opportunity_counts ON open_opportunity_counts.owner_id = owners.owner_id
ORDER BY open_opportunities DESC, leads_owned DESC, owner_name;
