-- Work order registration detail table pairing subject attributes with location and approval data for gdgs 2025.
{{ config(
  materialized='table',
    tags=["analytics","analytics_gdgs_2025", "registrations_gdgs_2025"]
) }}

SELECT 
w.*,
-- we.encounter_date_time as endline_date,
l.*,
a.approval_status

FROM 
{{ ref('dim_subjects_work_order_gdgs_25') }} AS w
LEFT JOIN 
{{ ref('location_gdgs_25') }} AS l
    ON w.location_id = l.address_id
LEFT JOIN 
{{ ref('approval_status_gdgs_25') }} AS a
    ON w.subject_id = a.entity_id

WHERE w.voided != TRUE


