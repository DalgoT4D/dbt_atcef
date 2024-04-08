
{{ config(
  materialized='table'
) }}


select date_time,
       state, 
       village, 
       taluka, 
       district, 
       dam as waterbodies, 
       project_ongoing, 
       project_not_started 
from {{ ref('work_order_union') }} 