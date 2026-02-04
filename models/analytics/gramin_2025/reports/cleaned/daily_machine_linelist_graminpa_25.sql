-- Filters the daily machine approval linelist down to encounters where the approval flag is set to Approved.
{{ config(
  materialized='table',
  tags=["analytics","analytics_graminpa_2025", "graminpa_2025", "graminpa", "analytical_models", "reports_graminpa_2025", "cleaned_graminpa_25"]
) }}

SELECT *
FROM {{ ref('daily_machine_linelist_approval_graminpa_25') }}
WHERE encounter_approval_status = 'Approved'
