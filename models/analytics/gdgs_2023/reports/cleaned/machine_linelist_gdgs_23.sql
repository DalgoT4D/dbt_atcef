-- Reuses the machine approval linelist and keeps only approved registrations.
{{ config(
  materialized='table',
  tags=["analytics", "analytics_gdgs_2023", "reports_gdgs_2023", "cleaned_gdgs_23"]
) }}

SELECT *
FROM {{ ref('machine_linelist_approval_gdgs_23') }}
WHERE approval_status = 'Approved'
