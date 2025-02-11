{{ config(
  materialized='table',
  tags=["final","final_niti_2023", "niti_2023", "niti"]
) }}

select 
       date_time,
       first_name,
       state,
       district,
       taluka,
       village,
       ngo_name,
       cast(total_silt_excavated_by_GP_for_non_farm_purpose as numeric) as total_silt_excavated_by_GP_for_non_farm_purpose
from {{ref('work_order_2023')}}
where encounter_type = 'Gram Panchayat Endline' and 
      cast(total_silt_excavated_by_GP_for_non_farm_purpose as numeric) != 0
