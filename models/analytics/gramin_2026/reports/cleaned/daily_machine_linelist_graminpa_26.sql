-- Filters the daily machine approval linelist down to encounters where the approval flag is set to Approved.
{{ config(
  materialized='table',
  tags=["analytics","analytics_gramin_2026", "analytical_models", "reports_graminpa_2026", "cleaned_graminpa_26"]
) }}

SELECT * -- date_time source: machine daily-recording encounter date from the approval model; safe to change/remove there.
FROM {{ ref('daily_machine_linelist_approval_graminpa_26') }}
-- WHERE encounter_approval_status = 'Approved'
WHERE approval_status = 'Approved' -- this is the specific encounter approval status
