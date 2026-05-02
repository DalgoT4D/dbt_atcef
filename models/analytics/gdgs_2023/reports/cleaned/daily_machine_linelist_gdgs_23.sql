-- Filters the daily machine approval linelist down to encounters where the approval flag is set to Approved.
{{ config(
  materialized='table',
  tags=["analytics","analytics_gdgs_2023", "analytical_models", "reports_gdgs_2023", "cleaned_gdgs_23"]
) }}

SELECT *
FROM {{ ref('daily_machine_linelist_approval_gdgs_23') }}
-- WHERE encounter_approval_status = 'Approved'
WHERE approval_status = 'Approved' -- this is the specific encounter approval status

