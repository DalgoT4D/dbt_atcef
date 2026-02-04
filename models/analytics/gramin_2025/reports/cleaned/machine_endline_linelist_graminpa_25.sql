-- Filters the machine endline approval linelist down to encounters marked Approved.
{{ config(
  materialized='table',
  tags=["analytics", "graminpa_2025", "graminpa", "analytical_models", "reports_graminpa_2025", "cleaned_graminpa_25"]
) }}

SELECT *
FROM {{ ref('machine_endline_linelist_approval_graminpa_25') }}
WHERE approval_status = 'Approved'
