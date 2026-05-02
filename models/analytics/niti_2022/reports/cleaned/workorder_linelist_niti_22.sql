-- Reuses the work order approval linelist and filters to approved registrations.
{{ config(
  materialized='table',
  tags=["analytics","analytics_niti_2022", "analytical_models", "reports_niti_2022"]
) }}


SELECT *

FROM {{ ref('workorder_linelist_approval_niti_22') }}
WHERE approval_status = 'Approved'
