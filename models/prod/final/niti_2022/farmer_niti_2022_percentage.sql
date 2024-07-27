{{ config(
  materialized='table'
) }}

SELECT
    state,
    district,
    taluka,
    village,
    dam,
    ngo_name,
    'farmer_niti_2022' AS farmer_type,
    SUM(farmer_niti_2022) AS farmers_count
FROM
   {{ ref('farmer_agg_niti_22') }}
GROUP BY
    state,
    district,
    taluka,
    village,
    dam,
    ngo_name


