-- Reuses the approval linelist and keeps only records with approved farmer, work order, and encounter statuses.
{{ config(
  materialized='table',
  tags=["analytics","analytics_niti_2023",  "analytical_models", "reports_niti_2023"]
) }}


SELECT *
FROM {{ ref('daily_farmer_linelist_approval_niti_23') }}
WHERE 
work_order_approval_status = 'Approved'
  AND farmer_approval_status = 'Approved'
  AND approval_status = 'Approved' -- this is the specific encounter approval status
