{{ config(
  materialized='table'
) }}

SELECT * FROM prod.work_order_2022
UNION 
SELECT * FROM prod.work_order_2023