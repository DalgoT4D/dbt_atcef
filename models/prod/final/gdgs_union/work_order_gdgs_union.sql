{{ config(
  materialized='table',
  tags=["final","final_gdgs_union"]
) }}

SELECT * FROM {{ ref('work_order_2024') }} 
UNION 
SELECT * FROM {{ ref('work_order_gdgs_2023') }} 