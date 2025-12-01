{{ config(
  materialized='table',
  tags=["analytics","analytics_niti_2025", "niti_2025", "niti", "niti_registrations"]
) }}

SELECT 
m.*,
l.*,
a.approval_status

FROM 
{{ ref('dim_subjects_machine_niti_25') }} AS m
LEFT JOIN 
{{ ref('location_niti_25') }} AS l
    ON m.location_id = l.address_id
LEFT JOIN 
{{ ref('approval_status_niti_25') }} AS a
    ON m.subject_id = a.entity_id

WHERE m.voided != TRUE