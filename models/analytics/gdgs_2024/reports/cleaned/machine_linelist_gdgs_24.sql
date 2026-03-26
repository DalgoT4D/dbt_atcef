-- Reuses the machine approval linelist and keeps only approved registrations.
{{ config(
  materialized='table',
  tags=["analytics", "analytics_gdgs_2024", "reports_gdgs_2024", "cleaned_gdgs_24"]
) }}

SELECT *
FROM {{ ref('machine_linelist_approval_gdgs_24') }}
WHERE approval_status = 'Approved'
