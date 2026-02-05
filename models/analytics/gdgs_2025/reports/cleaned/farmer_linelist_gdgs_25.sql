-- Shows approved farmer progress from the status_linelists table.
{{ config(
  materialized='table',
  tags=["analytics","analytical_models", "reports_gdgs_2025", "cleaned_gdgs_25"]
) }}

SELECT *
FROM {{ ref('farmer_linelist_approval_gdgs_25') }}
WHERE approval_status = 'Approved'

