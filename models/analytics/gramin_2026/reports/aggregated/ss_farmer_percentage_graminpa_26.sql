-- Legacy-compatible farmer category split for cross-year unions.
{{ config(
  materialized='table',
  tags=["analytics", "analytics_gramin_2026", "ss_2026", "ss_graminpa_2026"]
) }}

SELECT
    state,
    district,
    taluka,
    village,
    dam,
    ngo_name,
    'vulnerable' AS farmer_type,
    SUM(vulnerable_small + vulnerable_marginal) AS farmers_count
FROM {{ ref('ss_farmer_agg_graminpa_26') }}
GROUP BY state, district, taluka, village, dam, ngo_name
HAVING SUM(vulnerable_small + vulnerable_marginal) > 0

UNION ALL

SELECT
    state,
    district,
    taluka,
    village,
    dam,
    ngo_name,
    'others' AS farmer_type,
    SUM(semi_medium + medium + large) AS farmers_count
FROM {{ ref('ss_farmer_agg_graminpa_26') }}
GROUP BY state, district, taluka, village, dam, ngo_name
HAVING SUM(semi_medium + medium + large) > 0
