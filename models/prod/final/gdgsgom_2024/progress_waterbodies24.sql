{{ config(
  materialized='table'
) }}

SELECT DISTINCT ON (dam) dam AS waterbodies,
       date_time,
       state, 
       village, 
       taluka, 
       district, 
       encounter_type,
       CONCAT(
          CASE WHEN project_ongoing IS NOT NULL THEN 'Ongoing' ELSE '' END,
          CASE WHEN project_not_started IS NOT NULL THEN 'Not Started' ELSE '' END,
          CASE WHEN project_completed IS NOT NULL THEN 'Completed' ELSE '' END
       ) AS project_status
FROM {{ ref('work_order_2024') }}
Where dam is not null
ORDER BY dam, date_time