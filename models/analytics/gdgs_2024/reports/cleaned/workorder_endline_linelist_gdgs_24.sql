-- Pulls from the approval linelist and filters to work orders and endlines that are both approved.
{{ config(
  materialized='table',
  tags=["analytics","analytics_gdgs_2024", "analytical_models", "reports_gdgs_2024", "cleaned_gdgs_24"]
) }}

SELECT *
FROM {{ ref('workorder_endline_linelist_approval_gdgs_24') }}
WHERE approval_status = 'Approved'
