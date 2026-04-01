-- Filters the daily machine approval linelist down to encounters where the approval flag is set to Approved.
{{ config(
  materialized='table',
  tags=["analytics","analytics_niti_2024","analytical_models", "reports_niti_2024"]
) }}

SELECT *
FROM {{ ref('daily_machine_linelist_approval_niti_24') }}
  -- WHERE encounter_approval_status = 'Approved'
WHERE approval_status = 'Approved' -- this is the specific encounter approval status
