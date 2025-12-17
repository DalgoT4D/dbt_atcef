-- Filters the daily machine approval linelist down to encounters where the approval flag is set to Approved.
{{ config(
  materialized='table',
  tags=["analytics","analytics_niti_2025", "niti_2025", "niti", "analytical_models", "reports_niti_2025"]
) }}

SELECT *
FROM {{ ref('daily_machine_linelist_approval_niti_25') }}
WHERE encounter_approval_status = 'Approved'
