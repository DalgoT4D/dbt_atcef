-- Reuses the work order approval linelist and filters to approved registrations.
{{ config(
  materialized='table',
  tags=["analytics","analytics_graminpa_2025", "graminpa_2025", "graminpa", "analytical_models", "reports_graminpa_2025"]
) }}


SELECT *

FROM {{ ref('workorder_linelist_approval_graminpa_25') }}
WHERE approval_status = 'Approved'
