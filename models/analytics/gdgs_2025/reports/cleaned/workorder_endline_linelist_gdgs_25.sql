-- Pulls from the approval linelist and filters to work orders and endlines that are both approved.
{{ config(
  materialized='table',
  tags=["analytics","analytics_gdgs_2025", "gdgs_2025", "gdgs", "analytical_models", "reports_gdgs_2025", "cleaned_gdgs_25"]
) }}

SELECT *
FROM {{ ref('workorder_endline_linelist_approval_gdgs_25') }}
WHERE approval_status = 'Approved'
