{{ config(
  materialized='table',
  tags=["final","final_gramin_niti"]
) }}

SELECT 
m.farmer_id,
m.farmer_name,
m.state,
m.taluka,
m.village,
m.dam,
m.district,
e.type_of_land_silt_is_spread_on,
e.total_farm_area_silt_is_spread_on,
e.distance_from_waterbody,
CASE 
        WHEN e.encounter_type = 'Farmer Endline' THEN 'Endline Done'
        ELSE 'Endline Not Done'
    END AS endline_status
FROM {{ref('farmer_gramin')}} AS m 
LEFT JOIN {{ref('encounters_gramin')}} AS e 
ON m.farmer_id = e.subject_id 