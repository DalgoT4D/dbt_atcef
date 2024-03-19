{{ config(
  materialized='table'
) }}


SELECT
    state,
    district,
    taluka,
    village,
    dam,
    SUM(vulnerable_small + vulnerable_marginal) AS vulnerable_farmers_count,
    SUM(vulnerable_small + vulnerable_marginal + semi_medium + medium + large) AS total_farmers_count,
    ROUND(
        (SUM(vulnerable_small + vulnerable_marginal)::FLOAT / NULLIF(SUM(vulnerable_small + vulnerable_marginal + semi_medium + medium + large), 0))::NUMERIC, 2
    ) AS vulnerable_farmers_percentage
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
    dam
