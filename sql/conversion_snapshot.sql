SELECT
  'lead_lifecycle' AS metric_group,
  lifecycle_stage AS metric_name,
  COUNT(*) AS record_count,
  NULL::DOUBLE AS total_amount
FROM leads
GROUP BY lifecycle_stage
UNION ALL
SELECT
  'opportunity_stage' AS metric_group,
  stage AS metric_name,
  COUNT(*) AS record_count,
  COALESCE(SUM(amount), 0) AS total_amount
FROM opportunities
GROUP BY stage
ORDER BY metric_group, metric_name;
