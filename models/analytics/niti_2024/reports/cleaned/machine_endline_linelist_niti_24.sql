-- Filters the machine endline approval linelist down to encounters marked Approved.
{{ config(
  materialized='table',
  tags=["analytics", "analytics_niti_2024", "analytical_models", "reports_niti_2024"]
) }}

SELECT *
FROM {{ ref('machine_endline_linelist_approval_niti_24') }}
WHERE approval_status = 'Approved'
