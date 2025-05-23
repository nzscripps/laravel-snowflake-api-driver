-- Test complex structure similar to original query
WITH dates AS (
  SELECT TO_DATE('2025-04-01') AS cal_date
  UNION ALL
  SELECT TO_DATE('2025-04-08') AS cal_date
),
metrics1 AS (
  SELECT 
    TO_DATE('2025-04-01') AS date,
    100 AS value1
),
metrics2 AS (
  SELECT 
    TO_DATE('2025-04-01') AS date,
    'TEST' AS name,
    200 AS value2
)

SELECT 
  ARRAY_AGG(DATE_CATEGORY) WITHIN GROUP (ORDER BY DATE_CATEGORY ASC) AS DATE_CATEGORY,
  ARRAY_AGG(VALUE1) WITHIN GROUP (ORDER BY DATE_CATEGORY ASC) AS VALUE1,
  ARRAY_AGG(VALUE2) WITHIN GROUP (ORDER BY DATE_CATEGORY ASC) AS VALUE2
FROM (
  SELECT DISTINCT
    DATE_TRUNC('WEEK', dates.cal_date) AS DATE_CATEGORY,
    SUM(CASE WHEN m1.value1 IS NULL THEN 0 ELSE m1.value1 END) AS VALUE1,
    SUM(CASE WHEN m2.value2 IS NULL THEN 0 ELSE m2.value2 END) AS VALUE2
  FROM dates
  LEFT JOIN metrics1 m1 ON TO_DATE(m1.date) = dates.cal_date
  LEFT JOIN metrics2 m2 ON TO_DATE(m2.date) = dates.cal_date
  WHERE dates.cal_date BETWEEN '2025-04-01' AND '2025-04-22'
  GROUP BY DATE_CATEGORY
  ORDER BY DATE_CATEGORY
) subquery; 