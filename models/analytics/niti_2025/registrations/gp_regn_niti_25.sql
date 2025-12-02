{{ config(
  materialized='table',
  tags=["analytics","analytics_niti_2025", "niti_2025", "niti", "niti_registrations", "registrations_niti_2025"]
) }}

SELECT 
g.*,
a.approval_status

FROM 
{{ ref('dim_subjects_gp_niti_25') }} AS g
LEFT JOIN 
{{ ref('approval_status_niti_25') }} AS a
    ON g.subject_id = a.entity_id

WHERE g.voided != TRUE