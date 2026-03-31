SELECT
  account_id,
  account_name,
  company_domain,
  segment,
  owner_id
FROM accounts
WHERE segment = 'Enterprise'
  AND employee_count IS NULL
ORDER BY account_name;
