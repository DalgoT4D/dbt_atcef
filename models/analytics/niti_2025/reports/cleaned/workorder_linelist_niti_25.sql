-- Reuses the work order approval linelist and filters to approved registrations.
{{ config(
  materialized='table',
  tags=["analytics","analytics_niti_2025", "analytical_models", "reports_niti_2025"]
) }}


SELECT *

FROM {{ ref('workorder_linelist_approval_niti_25') }}
WHERE approval_status = 'Approved'
