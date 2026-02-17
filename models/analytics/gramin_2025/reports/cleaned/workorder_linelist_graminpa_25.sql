-- Reuses the work order approval linelist and filters to approved registrations.
{{ config(
  materialized='table',
  tags=["analytics","analytics_gramin_2025","analytics_graminpa_2025", "analytical_models", "reports_graminpa_2025", "cleaned_graminpa_25"]
) }}


SELECT *

FROM {{ ref('workorder_linelist_approval_graminpa_25') }}
WHERE approval_status = 'Approved'
