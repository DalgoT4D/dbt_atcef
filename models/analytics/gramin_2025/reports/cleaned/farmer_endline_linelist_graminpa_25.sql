-- Filters the approval linelist down to the farmers whose latest endline is approved.
{{ config(
  materialized='table',
  tags=["analytics","analytics_graminpa_2025", "graminpa_2025", "graminpa", "analytical_models", "reports_graminpa_2025"]
) }}


SELECT *
FROM {{ ref('farmer_endline_linelist_approval_graminpa_25') }}
WHERE approval_status = 'Approved'
