{{ config(
  materialized='table',
  tags=["final","final_gramin_niti"]
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
FROM
   {{ ref('farmer_agg_gramin') }}
GROUP BY
    state,
    district,
    taluka,
    village,
    dam,
    ngo_name
HAVING
    SUM(vulnerable_small + vulnerable_marginal) > 0

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
FROM
    {{ ref('farmer_agg_gramin') }}
GROUP BY
    state,
    district,
    taluka,
    village,
    dam,
    ngo_name
HAVING
    SUM(semi_medium + medium + large) > 0

ORDER BY
    state,
    district,
    taluka,
    village,
    dam, farmer_type, ngo_name
