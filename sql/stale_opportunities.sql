WITH context AS (
  SELECT CAST(reference_time AS TIMESTAMPTZ) AS reference_time
  FROM bundle_context
)
SELECT
  opportunity_id,
  account_id,
  owner_id,
  stage,
  amount,
  last_stage_change_at,
  date_diff('day', CAST(last_stage_change_at AS TIMESTAMPTZ), context.reference_time) AS days_in_stage
FROM opportunities
CROSS JOIN context
WHERE stage NOT IN ('Closed Won', 'Closed Lost')
  AND (
    last_stage_change_at IS NULL
    OR CAST(last_stage_change_at AS TIMESTAMPTZ) < context.reference_time - INTERVAL 21 DAY
  )
ORDER BY days_in_stage DESC, amount DESC, opportunity_id;
