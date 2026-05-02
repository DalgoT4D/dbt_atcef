-- Filters the approval linelist down to the farmers whose latest endline is approved.
{{ config(
  materialized='table',
  tags=["analytics","analytics_niti_2023", "analytical_models", "reports_niti_2023"]
) }}


SELECT *
FROM {{ ref('farmer_endline_linelist_approval_niti_23') }}
WHERE approval_status = 'Approved'
