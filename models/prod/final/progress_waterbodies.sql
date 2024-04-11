
{{ config(
  materialized='table'
) }}


SELECT DISTINCT ON (dam) dam AS waterbodies,
       date_time,
       state, 
       village, 
       taluka, 
       district, 
       ngo_name,
       encounter_type,
       project_ongoing, 
       project_not_started,
       project_completed
FROM {{ ref('work_order_union') }}
ORDER BY dam, date_time