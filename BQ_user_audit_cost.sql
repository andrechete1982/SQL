--Total Query Usage by Month

SELECT
  DATE_TRUNC(DATE(creation_time), MONTH) AS month,
  SUM(total_bytes_processed) / 1e9 AS gb_scanned,
  SUM(total_bytes_processed) / 1e12 * 5 AS estimated_cost_usd
FROM
  region-us.INFORMATION_SCHEMA.JOBS_BY_PROJECT
WHERE
  statement_type = 'SELECT'
GROUP BY
  month
ORDER BY
  month DESC

--Total Query Usage by Day

SELECT
  DATE(creation_time) AS day,
  SUM(total_bytes_processed) / 1e9 AS gb_scanned,
  SUM(total_bytes_processed) / 1e12 * 5 AS estimated_cost_usd
FROM
  region-us.INFORMATION_SCHEMA.JOBS_BY_PROJECT
WHERE
  statement_type = 'SELECT'
GROUP BY
  day
ORDER BY
  day DESC

--Queries Triggered by Looker

SELECT
  DATE(creation_time) AS day,
  user_email,
  SUM(total_bytes_processed) / 1e9 AS gb_scanned,
  SUM(total_bytes_processed) / 1e12 * 5 AS estimated_cost_usd
FROM
  region-us.INFORMATION_SCHEMA.JOBS_BY_PROJECT
WHERE
  statement_type = 'SELECT'
  AND user_email LIKE '%looker%'
GROUP BY
  day, user_email
ORDER BY
  day DESC

--Bonus Query (Very Useful)

SELECT
  creation_time,
  user_email,
  total_bytes_processed / 1e9 AS gb_scanned,
  query
FROM
  region-us.INFORMATION_SCHEMA.JOBS_BY_PROJECT
WHERE
  statement_type = 'SELECT'
ORDER BY
  total_bytes_processed DESC
LIMIT 20
