-- Pulls from the approval linelist and filters to work orders and endlines that are both approved.
{{ config(
  materialized='table',
  tags=["analytics","analytics_graminpa_2025", "graminpa_2025", "graminpa", "analytical_models", "reports_graminpa_2025", "cleaned_graminpa_25"]
) }}

SELECT *
FROM {{ ref('workorder_endline_linelist_approval_graminpa_25') }}
WHERE approval_status = 'Approved'
