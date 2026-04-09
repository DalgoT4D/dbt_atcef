-- Gram Panchayat registration snapshot with approval context, location, and subject information for NITI 2023.
{{ config(
  materialized='table',
  tags=["analytics","analytics_niti_2023",  "niti_registrations", "registrations_niti_2023"]
) }}

SELECT 
g.*,
a.approval_status

FROM 
{{ ref('dim_subjects_gp_niti_23') }} AS g
LEFT JOIN 
{{ ref('approval_status_niti_23') }} AS a
    ON g.subject_id = a.entity_id

WHERE g.voided != TRUE
