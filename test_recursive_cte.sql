-- Test recursive CTE
WITH RECURSIVE nrows(cal_date) AS (
  SELECT to_date('2025-04-01')
  UNION ALL
  SELECT dateadd(day, 1, cal_date)
  FROM nrows
  WHERE cal_date < '2025-04-22'
)
SELECT cal_date FROM nrows; 