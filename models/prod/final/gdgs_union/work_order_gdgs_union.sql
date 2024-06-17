{{ config(
  materialized='table'
) }}

SELECT * FROM {{ ref('work_order_2024') }} 
UNION 
SELECT * FROM {{ ref('work_order_gdgs_2023') }} 