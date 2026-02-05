-- Filters the approval linelist down to the farmers whose latest endline is approved.
{{ config(
  materialized='table',
  tags=["analytics","analytics_gdgs_2025", "analytical_models", "reports_gdgs_2025", "cleaned_gdgs_25"]
) }}


SELECT *
FROM {{ ref('farmer_endline_linelist_approval_gdgs_25') }}
WHERE approval_status = 'Approved'
