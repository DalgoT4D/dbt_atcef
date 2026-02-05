-- Reuses the work order approval linelist and filters to approved registrations.
{{ config(
  materialized='table',
  tags=["analytics","analytics_gdgs_2025", "analytical_models", "reports_gdgs_2025", "cleaned_gdgs_25"]
) }}


SELECT *

FROM {{ ref('workorder_linelist_approval_gdgs_25') }}
WHERE approval_status = 'Approved'
