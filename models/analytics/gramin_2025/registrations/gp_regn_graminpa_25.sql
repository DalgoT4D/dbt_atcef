-- Gram Panchayat registration snapshot with approval context, location, and subject information for graminpa 2025.
{{ config(
  materialized='table',
  tags=["analytics","analytics_graminpa_2025", "graminpa_2025", "graminpa", "graminpa_registrations", "registrations_graminpa_2025"]
) }}

SELECT 
g.*,
a.approval_status

FROM 
{{ ref('dim_subjects_gp_graminpa_25') }} AS g
LEFT JOIN 
{{ ref('approval_status_graminpa_25') }} AS a
    ON g.subject_id = a.entity_id

WHERE g.voided != TRUE
