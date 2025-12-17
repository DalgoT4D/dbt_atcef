-- Reuses the machine approval linelist and keeps only approved registrations.
{{ config(
  materialized='table',
  tags=["analytics", "niti_2025", "niti", "analytical_models", "reports_niti_2025"]
) }}

SELECT *
FROM {{ ref('machine_linelist_approval_niti_25') }}
WHERE approval_status = 'Approved'
