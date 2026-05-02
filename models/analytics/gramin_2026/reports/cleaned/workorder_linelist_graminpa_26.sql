-- Reuses the work order approval linelist and filters to approved registrations.
{{ config(
  materialized='table',
  tags=["analytics","analytics_gramin_2026","analytics_graminpa_2026", "analytical_models", "reports_graminpa_2026", "cleaned_graminpa_26"]
) }}


SELECT *

FROM {{ ref('workorder_linelist_approval_graminpa_26') }}
WHERE approval_status = 'Approved'
