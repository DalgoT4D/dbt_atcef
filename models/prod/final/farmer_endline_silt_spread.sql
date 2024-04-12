{{ config(
  materialized='table'
) }}


SELECT
    date_time,
    state, 
    district,
    taluka,
    village,
    dam,
    ngo_name,
    CAST(SUM(total_farm_area_on_which_Silt_is_spread) AS NUMERIC(10, 2)) AS total_farm_area_on_which_Silt_is_spread
FROM
    {{ ref('work_order_union') }}
WHERE
    encounter_type = 'Farmer Endline'
GROUP BY
    state,
    district,
    taluka,
    village,
    dam,
    date_time,
    ngo_name
