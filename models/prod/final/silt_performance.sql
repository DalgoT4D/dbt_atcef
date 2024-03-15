{{ config(
  materialized='table'
) }}


WITH ranked_districts AS (
  SELECT
    district,
    AVG(pct_silt_achieved_vs_target) AS avg_pct_achieved
  FROM
  {{ ref('work_order') }}
  GROUP BY
    district
  ORDER BY
    avg_pct_achieved DESC
)

, top_performers AS (
  SELECT
    district,
    avg_pct_achieved,
    ROW_NUMBER() OVER(ORDER BY avg_pct_achieved DESC) AS rank
  FROM
    ranked_districts
  LIMIT 5
)

, bottom_performers AS (
  SELECT
    district,
    avg_pct_achieved,
    ROW_NUMBER() OVER(ORDER BY avg_pct_achieved ASC) AS rank
  FROM
    ranked_districts
  LIMIT 5
)

SELECT
  'Top Performers' AS performance_category,
  district,
  avg_pct_achieved
FROM
  top_performers

UNION ALL

SELECT
  'Bottom Performers' AS performance_category,
  district,
  avg_pct_achieved
FROM
  bottom_performers