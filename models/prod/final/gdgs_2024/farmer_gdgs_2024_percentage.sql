{{ config(
  materialized='table'
) }}

SELECT
    state,
    district,
    taluka,
    village,
    dam,
    'vulnerable' AS farmer_type,
    SUM(vulnerable_small + vulnerable_marginal + widow + disabled) AS farmers_count
FROM
    {{ref('farmer_agg_gdgs_24')}}
GROUP BY
    state,
    district,
    taluka,
    village,
    dam

UNION ALL

SELECT
    state,
    district,
    taluka,
    village,
    dam,
    'others' AS farmer_type,
    SUM(semi_medium + medium + large) AS farmers_count
FROM
    {{ref('farmer_agg_gdgs_24')}}
GROUP BY
    state,
    district,
    taluka,
    village,
    dam

ORDER BY
    state,
    district,
    taluka,
    village,
    dam, farmer_type
