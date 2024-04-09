{{ config(
  materialized='table'
) }}


WITH cte AS (
    SELECT DISTINCT ON (mobile_number)
        mobile_number,
        state,
        district,
        taluka,
        village,
        dam,
        date_time,
        category_of_farmer
    FROM
        {{ ref('work_order_union') }} 
    WHERE
        encounter_type = 'Farmer Endline'
        AND project_ongoing = 'Ongoing'
    ORDER BY
        mobile_number, date_time DESC
)
SELECT
    max(date_time) AS date_time,
    state,
    district,
    taluka,
    village,
    dam AS waterbodies,
    SUM(
        CASE
            WHEN category_of_farmer = 'Small (2.5-4.99 acres)' THEN 1
            ELSE 0
        END
    ) AS vulnerable_small,
    SUM(
        CASE
            WHEN category_of_farmer = 'Marginal (0-2.49 acres)' THEN 1
            ELSE 0
        END
    ) AS vulnerable_marginal,
    SUM(
        CASE
            WHEN category_of_farmer = 'Semi-medium (5-9.55 acres)' THEN 1
            ELSE 0
        END
    ) AS semi_medium,
    SUM(
        CASE
            WHEN category_of_farmer = 'Medium (10-24.99 acres)' THEN 1
            ELSE 0
        END
    ) AS medium,
    SUM(
        CASE
            WHEN category_of_farmer = 'Large (above 25 acres)' THEN 1
            ELSE 0
        END
    ) AS large
FROM
    cte
GROUP BY
    state,
    district,
    taluka,
    village,
    dam



