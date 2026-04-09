-- Reuses the work order approval linelist and filters to approved registrations.
{{ config(
  materialized='table',
  tags=["analytics","analytics_niti_2023", "analytical_models", "reports_niti_2023"]
) }}


SELECT *

FROM {{ ref('workorder_linelist_approval_niti_23') }}
WHERE approval_status = 'Approved'
