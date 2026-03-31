SELECT
  company_domain,
  COUNT(*) AS account_count,
  string_agg(account_id, ', ' ORDER BY account_id) AS account_ids,
  string_agg(DISTINCT owner_id, ', ' ORDER BY owner_id) AS owner_ids
FROM accounts
GROUP BY company_domain
HAVING COUNT(*) > 1
ORDER BY account_count DESC, company_domain;
