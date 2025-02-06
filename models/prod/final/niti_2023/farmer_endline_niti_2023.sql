{{ config(
  materialized='table',
  tags=["final","final_niti_2023"]
) }}

SELECT 
m.farmer_id,
m.farmer_name,
m.state,
m.taluka,
m.village,
m.dam,
m.distRict,
e.type_of_land_silt_is_spread_on,
e.total_farm_area_silt_is_spread_on,
e.distance_from_waterbody,
e.total_silt_carted,
CASE 
        WHEN e.total_farm_area_silt_is_spread_on > 0 
        THEN ROUND(e.total_silt_carted / NULLIF(e.total_farm_area_silt_is_spread_on, 0), 2) 
        ELSE NULL 
    END AS silt_per_acre,
CASE 
        WHEN e.total_farm_area_silt_is_spread_on > 0 
        AND (e.total_silt_carted / NULLIF(e.total_farm_area_silt_is_spread_on, 0)) >= 420 
        THEN 'Above Benchmark' 
        ELSE 'Below Benchmark' 
    END AS silt_per_acre_benchmark_classification,
CASE 
        WHEN e.encounter_type = 'Farmer Endline' THEN 'Endline Done'
        ELSE 'Endline Not Done'
    END AS endline_status
FROM {{ref('farmer_niti_2023')}} AS m 
LEFT JOIN {{ref('encounter_2023')}} AS e 
ON m.farmer_id = e.subject_id 