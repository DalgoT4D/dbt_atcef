-- Gram Panchayat registration snapshot with approval context, location, and subject information for NITI 2022.
{{ config(
  materialized='table',
  tags=["analytics","analytics_niti_2022",  "niti_registrations", "registrations_niti_2022"]
) }}

SELECT 
g.*,
a.approval_status

FROM 
{{ ref('dim_subjects_gp_niti_22') }} AS g
LEFT JOIN 
{{ ref('approval_status_niti_22') }} AS a
    ON g.subject_id = a.entity_id

WHERE g.voided != TRUE
