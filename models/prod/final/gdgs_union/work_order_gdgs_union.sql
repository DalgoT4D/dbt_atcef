{{ config(
  materialized='table',
  tags=["final","final_gdgs_union", "gdgs"]
) }}

SELECT * FROM {{ ref('work_order_2024') }} 
UNION ALL
SELECT * FROM {{ ref('work_order_gdgs_2023') }} 