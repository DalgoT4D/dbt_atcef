{{ config(
  materialized='table'
) }}

WITH waterbodies AS (
    SELECT 
        w.dam,
        w.work_order_id,
        w.state,
        w.village,
        w.district,
        w.taluka,
        e.encounter_type,
        e.date_time,
        ROW_NUMBER() OVER (PARTITION BY w.dam ORDER BY e.date_time DESC) AS row_num
    FROM {{ ref('work_order_gdgs_2024') }} AS w
    LEFT JOIN {{ ref('encounters_2024') }} AS e 
        ON e.subject_id = w.work_order_id
    WHERE e.encounter_type = 'Work order daily Recording - Farmer' 
       OR e.encounter_type = 'Work order endline'
)

SELECT 
    dam,
    work_order_id,
    state,
    village,
    district,
    taluka,
    encounter_type,
    date_time,
    CASE 
        WHEN encounter_type = 'Work order daily Recording - Farmer' THEN 'Ongoing'
        WHEN encounter_type = 'Work order endline' THEN 'Completed'
        ELSE NULL
    END AS project_status
FROM waterbodies
WHERE row_num = 1
