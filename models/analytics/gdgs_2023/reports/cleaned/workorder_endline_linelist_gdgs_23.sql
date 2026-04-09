-- Pulls from the approval linelist and filters to work orders and endlines that are both approved.
{{ config(
  materialized='table',
  tags=["analytics","analytics_gdgs_2023", "analytical_models", "reports_gdgs_2023", "cleaned_gdgs_23"]
) }}

SELECT *
FROM {{ ref('workorder_endline_linelist_approval_gdgs_23') }}
WHERE approval_status = 'Approved'
