{{ config(
  materialized='table',
  tags=["analytics","analytics_niti_2025", "niti_2025", "niti", "niti_registrations"]
) }}

SELECT 
f.*,
l.*,
a.approval_status

FROM 
{{ ref('dim_subjects_farmer_niti_25') }} AS f
LEFT JOIN 
{{ ref('location_niti_25') }} AS l
    ON f.location_id = l.address_id
LEFT JOIN 
{{ ref('approval_status_niti_25') }} AS a
    ON f.subject_id = a.entity_id

WHERE f.voided != TRUE
