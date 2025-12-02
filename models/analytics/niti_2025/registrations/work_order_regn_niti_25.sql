{{ config(
  materialized='table',
  tags=["analytics","analytics_niti_2025", "niti_2025", "niti", "niti_registrations", "registrations_niti_2025"]
) }}

SELECT 
w.*,
-- we.encounter_date_time as endline_date,
l.*,
a.approval_status

FROM 
{{ ref('dim_subjects_work_order_niti_25') }} AS w
LEFT JOIN 
{{ ref('location_niti_25') }} AS l
    ON w.location_id = l.address_id
LEFT JOIN 
{{ ref('approval_status_niti_25') }} AS a
    ON w.subject_id = a.entity_id

WHERE w.voided != TRUE



