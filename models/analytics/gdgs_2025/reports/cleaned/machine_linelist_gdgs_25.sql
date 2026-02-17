-- Reuses the machine approval linelist and keeps only approved registrations.
{{ config(
  materialized='table',
  tags=["analytics", "analytics_gdgs_2025", "reports_gdgs_2025", "cleaned_gdgs_25"]
) }}

SELECT *
FROM {{ ref('machine_linelist_approval_gdgs_25') }}
WHERE approval_status = 'Approved'
