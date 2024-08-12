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
        MAX(CASE WHEN e.encounter_type = 'Work order endline' THEN e.date_time END) AS endline_date,
        MAX(CASE WHEN e.encounter_type = 'Work order daily Recording - Farmer' THEN e.date_time END) AS farmer_date
    FROM  {{ ref('work_order_gdgs_23') }} AS w
    LEFT JOIN {{ ref('encounters_gdgs_2023') }} AS e 
        ON e.subject_id = w.work_order_id
    WHERE (e.encounter_type IN ('Work order daily Recording - Farmer', 'Work order endline')
           OR e.encounter_type IS NULL)  -- Ensure work orders with no encounters are included
       AND w.work_order_voided != TRUE
    GROUP BY 
        w.dam,
        w.work_order_id,
        w.state,
        w.village,
        w.district,
        w.taluka,
        e.subject_id
)


SELECT 
    dam,
    work_order_id,
    state,
    village,
    district,
    taluka,
    COALESCE(
        endline_date,
        farmer_date
    ) AS latest_encounter_date,
    CASE 
        WHEN endline_date IS NOT NULL THEN 'Completed'
        WHEN farmer_date IS NOT NULL THEN 'Ongoing'
        ELSE 'Not Started'
    END AS project_status,
    CASE 
        WHEN endline_date IS NOT NULL THEN 'Endline Done'
        ELSE 'Endline Not Done'
    END AS work_order_endline_status 
FROM waterbodies
