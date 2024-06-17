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
from {{ref('farmer_niti_2022_percentage')}}
 
UNION ALL 

select 
    state, 
    district,
    taluka,
    village,
    dam,
    farmer_type,
    farmers_count
from {{ref('farmer_niti_2023_percentage')}}