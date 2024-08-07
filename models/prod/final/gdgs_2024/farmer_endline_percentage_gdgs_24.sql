{{ config(
  materialized='table'
) }}

WITH cte AS (
    SELECT 
        f.farmer_id,
        f.farmer_name,
        f.dam,
        f.district,
        f.state,
        f.taluka,
        f.village,
        CASE 
            WHEN e.farmer_sub_id IS NOT NULL THEN 'Endline Done'
            ELSE 'Endline Not Done'
        END AS farmer_endline_status
    FROM 
        {{ref('farmer_gdgs_2024')}} AS f
    LEFT JOIN (
        SELECT DISTINCT farmer_sub_id
        FROM {{ref('encounters_2024')}}
        WHERE encounter_type = 'Farmer Endline'
    ) AS e
    ON 
        f.farmer_id = e.farmer_sub_id
    GROUP BY
        f.farmer_id, 
        f.farmer_name,
        f.dam,
        f.district,
        f.state,
        f.taluka,
        f.village,
        e.farmer_sub_id
),

excluded_farmers AS (
    SELECT DISTINCT farmer_id
    FROM {{ref('encounters_2024')}}
    WHERE encounter_type = 'Work order daily Recording - Farmer'
)

SELECT DISTINCT
    cte.farmer_id,
    cte.farmer_name,
    cte.dam,
    cte.district,
    cte.state,
    cte.taluka,
    cte.village,
    cte.farmer_endline_status
FROM 
    cte
LEFT JOIN 
    excluded_farmers AS ex
ON 
    cte.farmer_id = ex.farmer_id
WHERE 
    ex.farmer_id IS NULL

