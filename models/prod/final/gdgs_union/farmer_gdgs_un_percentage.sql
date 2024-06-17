{{ config(
  materialized='table'
) }}


select 
    state, 
    district,
    taluka,
    village,
    dam,
    farmer_type,
    farmers_count
from {{ref('farmer_gdgs_2023_percentage')}}
 
UNION ALL 

select 
    state, 
    district,
    taluka,
    village,
    dam,
    farmer_type,
    farmers_count
from {{ref('farmer_gdgs_2024_percentage')}}