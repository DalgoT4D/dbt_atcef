-- Gram Panchayat registration snapshot with approval context, location, and subject information for NITI 2024.
{{ config(
  materialized='table',
  tags=["analytics","analytics_niti_2024",  "niti_registrations", "registrations_niti_2024"]
) }}

SELECT 
g.*,
a.approval_status

FROM 
{{ ref('dim_subjects_gp_niti_24') }} AS g
LEFT JOIN 
{{ ref('approval_status_niti_24') }} AS a
    ON g.subject_id = a.entity_id

WHERE g.voided != TRUE
