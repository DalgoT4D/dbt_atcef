-- Shows approved farmer progress from the status_linelists table.
{{ config(
  materialized='table',
  tags=["analytics", "niti_2025", "niti", "analytical_models", "reports_niti_2025"]
) }}

SELECT *
FROM {{ ref('farmer_linelist_approval_niti_25') }}
WHERE approval_status = 'Approved'

