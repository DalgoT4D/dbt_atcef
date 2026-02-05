-- Reuses the approval linelist and keeps only records with approved farmer, work order, and encounter statuses.
{{ config(
  materialized='table',
  tags=["analytics","analytics_gdgs_2025", "gdgs_2025", "gdgs", "analytical_models", "reports_gdgs_2025", "cleaned_gdgs_25"]
) }}


SELECT *
FROM {{ ref('daily_farmer_linelist_approval_gdgs_25') }}
WHERE work_order_approval_status = 'Approved'
  AND farmer_approval_status = 'Approved'
  AND encounter_approval_status = 'Approved'
