-- Filters the approval linelist down to the farmers whose latest endline is approved.
{{ config(
  materialized='table',
  tags=["analytics","analytics_gramin_2026","analytical_models", "reports_graminpa_2026", "cleaned_graminpa_26"]
) }}


SELECT * -- date_time source: farmer endline encounter date from the approval model; safe to change/remove there.
FROM {{ ref('farmer_endline_linelist_approval_graminpa_26') }}
WHERE approval_status = 'Approved'
