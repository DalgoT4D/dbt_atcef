-- Filters the machine endline approval linelist down to encounters marked Approved.
{{ config(
  materialized='table',
  tags=["analytics", "analytics_niti_2023", "analytical_models", "reports_niti_2023"]
) }}

SELECT *
FROM {{ ref('machine_endline_linelist_approval_niti_23') }}
WHERE approval_status = 'Approved'
