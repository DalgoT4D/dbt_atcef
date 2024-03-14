{{ config(
  materialized='table'
) }}

SELECT * FROM {{ ref('work_order_2022') }} 
UNION 
SELECT * FROM {{ ref('work_order_2023') }} 