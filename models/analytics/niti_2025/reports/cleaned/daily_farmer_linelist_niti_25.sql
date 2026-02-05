-- Reuses the approval linelist and keeps only records with approved farmer, work order, and encounter statuses.
{{ config(
  materialized='table',
  tags=["analytics","analytics_niti_2025", "niti_2025", "niti", "analytical_models", "reports_niti_2025"]
) }}


SELECT *
FROM {{ ref('daily_farmer_linelist_approval_niti_25') }}
WHERE 
-- work_order_approval_status = 'Approved'
--   AND farmer_approval_status = 'Approved'
  -- AND 
  encounter_approval_status = 'Approved'
