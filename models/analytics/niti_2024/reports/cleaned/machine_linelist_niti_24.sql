-- Reuses the machine approval linelist and keeps only approved registrations.
{{ config(
  materialized='table',
  tags=["analytics", "analytics_niti_2024",  "analytical_models", "reports_niti_2024"]
) }}

SELECT *
FROM {{ ref('machine_linelist_approval_niti_24') }}
WHERE approval_status = 'Approved'
