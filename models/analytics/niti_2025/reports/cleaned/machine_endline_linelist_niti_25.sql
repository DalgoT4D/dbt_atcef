-- Filters the machine endline approval linelist down to encounters marked Approved.
{{ config(
  materialized='table',
  tags=["analytics", "niti_2025", "niti", "analytical_models", "reports_niti_2025"]
) }}

SELECT *
FROM {{ ref('machine_endline_linelist_approval_niti_25') }}
WHERE approval_status = 'Approved'
