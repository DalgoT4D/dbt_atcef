-- Work order registration detail table pairing subject attributes 
-- with location and approval data for graminpa 2026.
{{ config(
  materialized='table',
    tags=["analytics","analytics_gramin_2026","analytics_graminpa_2026", "registrations_gramin_2026"]
) }}

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
        {{ ref('dim_subjects_work_order_graminpa_26') }} AS w
    LEFT JOIN 
        {{ ref('location_graminpa_26') }} AS l
            ON w.location_id = l.address_id
    LEFT JOIN 
        {{ ref('approval_status_graminpa_26') }} AS a
            ON w.subject_id = a.entity_id
    WHERE w.voided != TRUE
)

SELECT *
FROM base
WHERE rn = 1

