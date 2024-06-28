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
    SUM(vulnerable_small + vulnerable_marginal + widow + disabled + family_of_farmer_who_committed_suicide) AS farmers_count
FROM
    {{ref('farmer_agg_gdgs_24')}}
GROUP BY
    state,
    district,
    taluka,
    village,
    dam,
    ngo_name

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
    {{ref('farmer_agg_gdgs_24')}}
GROUP BY
    state,
    district,
    taluka,
    village,
    dam,
    ngo_name

ORDER BY
    state,
    district,
    taluka,
    village,
    dam, 
    ngo_name,
    farmer_type
