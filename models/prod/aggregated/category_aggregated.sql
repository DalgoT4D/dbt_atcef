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
    SUM(vulnerable_small + vulnerable_marginal) AS farmers_count
FROM
    {{ ref('category_farmer') }}
GROUP BY
    state,
    district,
    taluka,
    village,
    dam
HAVING
    SUM(vulnerable_small + vulnerable_marginal) > 0

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
    {{ ref('category_farmer') }}
GROUP BY
    state,
    district,
    taluka,
    village,
    dam
HAVING
    SUM(vulnerable_small + vulnerable_marginal + semi_medium + medium + large) > 0

ORDER BY
    state,
    district,
    taluka,
    village,
    dam, farmer_type
