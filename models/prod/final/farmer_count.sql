{{ config(
  materialized='table'
) }}


SELECT
    state,
    district,
    taluka,
    village,
    waterbodies,
    'vulnerable' AS farmer_type,
    SUM(vulnerable_small + vulnerable_marginal) AS farmers_count
FROM
   {{ ref('farmer_endline') }}
GROUP BY
    state,
    district,
    taluka,
    village,
    waterbodies
HAVING
    SUM(vulnerable_small + vulnerable_marginal) > 0

UNION ALL

SELECT
    state,
    district,
    taluka,
    village,
    waterbodies,
    'others' AS farmer_type,
    SUM(semi_medium + medium + large) AS farmers_count
FROM
    {{ ref('farmer_endline') }}
GROUP BY
    state,
    district,
    taluka,
    village,
    waterbodies
HAVING
    SUM(vulnerable_small + vulnerable_marginal + semi_medium + medium + large) > 0

ORDER BY
    state,
    district,
    taluka,
    village,
    waterbodies, farmer_type
