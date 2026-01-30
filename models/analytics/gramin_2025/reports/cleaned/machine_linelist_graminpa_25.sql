-- Reuses the machine approval linelist and keeps only approved registrations.
{{ config(
  materialized='table',
  tags=["analytics", "graminpa_2025", "graminpa", "analytical_models", "reports_graminpa_2025"]
) }}

SELECT *
FROM {{ ref('machine_linelist_approval_graminpa_25') }}
WHERE approval_status = 'Approved'
