{{ config(
  materialized='table'
) }}

WITH address_cte AS (
    SELECT * 
    FROM {{ ref('address_niti_2023') }}
), 
registered_waterbodies AS (
    SELECT * 
    FROM {{ ref('work_order_niti_2023') }}
),
waterbodies AS (
    SELECT 
        a.dam,
        w.work_order_id,
        a.state,
        a.village,
        a.district,
        a.taluka,
        a.stakeholder_responsible as ngo_name,
        e.encounter_type,
        e.date_time,
        ROW_NUMBER() OVER (PARTITION BY a.dam ORDER BY e.date_time DESC) AS row_num
    FROM address_cte AS a
    LEFT JOIN registered_waterbodies AS w ON a.dam = w.dam
    LEFT JOIN {{ ref('encounter_2023') }} AS e ON e.subject_id = w.work_order_id
        AND (e.encounter_type = 'Work order daily Recording - Farmer' 
             OR e.encounter_type = 'Work order endline')
),

waterbodies_active as (SELECT 
    wb.dam,
    wb.work_order_id,
    wb.state,
    wb.village,
    wb.district,
    wb.taluka,
    wb.ngo_name,
    wb.encounter_type,
    wb.date_time,
    CASE 
        WHEN wb.encounter_type = 'Work order daily Recording - Farmer' THEN 'Ongoing'
        WHEN wb.encounter_type = 'Work order endline' THEN 'Completed'
        WHEN wb.encounter_type IS NULL THEN 'Not Started'
        ELSE NULL
    END AS project_status
FROM waterbodies wb
WHERE row_num = 1
   OR (wb.encounter_type IS NULL AND row_num = 1))

Select * from waterbodies_active where project_status is not null