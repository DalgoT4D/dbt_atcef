{{ config(
  materialized='table'
) }}


select 
    state,
    district,
    taluka,
    village,
    dam,
    date_time,
    mobile_verified_count,
    mobile_unverified_count,
    (mobile_verified_count + mobile_unverified_count) as total
from
{{ ref('category_farmer') }} 