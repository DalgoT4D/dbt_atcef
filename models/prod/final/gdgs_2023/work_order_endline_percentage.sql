{{ config(
  materialized='table'
) }}

WITH cte AS (
    SELECT 
        w.dam,
        w.district,
        w.state,
        w.taluka,
        w.village,
        w.ngo_name,
        CASE 
            WHEN e.subject_id IS NOT NULL THEN 'Endline Done'
            ELSE 'Endline Not Done'
        END AS work_order_endline_status
    FROM 
        {{ref('work_order_gdgs_23')}} AS w
    LEFT JOIN (
        SELECT DISTINCT subject_id
        FROM {{ref('encounters_gdgs_2023')}}
        WHERE encounter_type = 'Work order endline'
    ) AS e
    ON 
        w.work_order_id = e.subject_id
    GROUP BY 
        w.dam,
        w.district,
        w.state,
        w.taluka,
        w.village,
        w.ngo_name,
        e.subject_id
)

SELECT DISTINCT
    dam,
    district,
    state,
    taluka,
    village,
    ngo_name,
    work_order_endline_status
FROM 
    cte

