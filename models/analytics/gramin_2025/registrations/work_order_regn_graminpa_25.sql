-- Work order registration detail table pairing subject attributes with location and approval data for graminpa 2025.
{{ config(
  materialized='table',
    tags=["analytics","analytics_graminpa_2025", "registrations_graminpa_2025"]
) }}

-- SELECT 
-- w.*,
-- -- we.encounter_date_time as endline_date,
-- l.*,
-- a.approval_status

-- FROM 
-- {{ ref('dim_subjects_work_order_graminpa_25') }} AS w
-- LEFT JOIN 
-- {{ ref('location_graminpa_25') }} AS l
--     ON w.location_id = l.address_id
-- LEFT JOIN 
-- {{ ref('approval_status_graminpa_25') }} AS a
--     ON w.subject_id = a.entity_id

-- WHERE w.voided != TRUE


WITH base AS (
    SELECT
        w.*,
        l.*,
        a.approval_status,
        ROW_NUMBER() OVER (
            PARTITION BY w.subject_id
            ORDER BY w.registration_date DESC NULLS LAST
        ) AS rn
    FROM 
        {{ ref('dim_subjects_work_order_graminpa_25') }} AS w
    LEFT JOIN 
        {{ ref('location_graminpa_25') }} AS l
            ON w.location_id = l.address_id
    LEFT JOIN 
        {{ ref('approval_status_graminpa_25') }} AS a
            ON w.subject_id = a.entity_id
    WHERE w.voided != TRUE
)

SELECT *
FROM base
WHERE rn = 1

