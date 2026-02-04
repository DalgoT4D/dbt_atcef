{{ config(
  materialized='table',
  tags=["analytics", "analytical_models", "reports_overall_2025"]
) }}

SELECT *, 'niti 2025' AS project
FROM {{ ref('dam_wise_work_order_niti_25') }}
UNION ALL
SELECT *, 'gdgs 2025' AS project
FROM {{ ref('dam_wise_work_order_gdgs_25') }}
UNION ALL
SELECT *, 'graminpa 2025' AS project
FROM {{ ref('dam_wise_work_order_graminpa_25') }}