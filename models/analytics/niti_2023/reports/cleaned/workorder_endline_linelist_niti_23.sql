-- Pulls from the approval linelist and filters to work orders and endlines that are both approved.
{{ config(
  materialized='table',
  tags=["analytics","analytics_niti_2023","analytical_models", "reports_niti_2023"]
) }}

SELECT *
FROM {{ ref('workorder_endline_linelist_approval_niti_23') }}
WHERE approval_status = 'Approved'
