-- Gram Panchayat registration snapshot with approval context, location, and subject information for graminpa 2026.
{{ config(
  materialized='table',
  tags=["analytics", "analytics_gramin_2026","graminpa_registrations", "registrations_gramin_2026"]
) }}

SELECT 
g.*,
a.approval_status

FROM 
{{ ref('dim_subjects_gp_graminpa_26') }} AS g
LEFT JOIN 
{{ ref('approval_status_graminpa_26') }} AS a
    ON g.subject_id = a.entity_id

WHERE g.voided != TRUE
