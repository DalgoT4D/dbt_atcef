
{{ config(
  materialized='table'
) }}


SELECT DISTINCT ON (dam) dam AS waterbodies,
       date_time,
       state, 
       village, 
       taluka, 
       district, 
       project_ongoing, 
       project_not_started 
FROM {{ ref('work_order_union') }}
ORDER BY dam, date_time