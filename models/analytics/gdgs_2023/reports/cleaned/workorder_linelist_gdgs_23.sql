-- Reuses the work order approval linelist and filters to approved registrations.
{{ config(
  materialized='table',
  tags=["analytics","analytics_gdgs_2023", "analytical_models", "reports_gdgs_2023", "cleaned_gdgs_23"]
) }}


SELECT *

FROM {{ ref('workorder_linelist_approval_gdgs_23') }}
WHERE approval_status = 'Approved'
