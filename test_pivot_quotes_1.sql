-- First query: Test PIVOT syntax with quoted beacon IDs
WITH test_data AS (
  SELECT 1 AS ID, '9e90t3nOfBJS3oQuFn7MzI3v0G1s' AS BEACON_ID, 10 AS DIFF_FRAC_FLOOR
  UNION ALL
  SELECT 2 AS ID, 'Wzlyy2fuuDSUiTmzhqIq4dGVV1QB' AS BEACON_ID, 20 AS DIFF_FRAC_FLOOR
)
SELECT * FROM (
  SELECT ID, BEACON_ID, DIFF_FRAC_FLOOR
  FROM test_data
)
PIVOT(SUM(DIFF_FRAC_FLOOR) FOR BEACON_ID IN ('9e90t3nOfBJS3oQuFn7MzI3v0G1s','Wzlyy2fuuDSUiTmzhqIq4dGVV1QB')); 