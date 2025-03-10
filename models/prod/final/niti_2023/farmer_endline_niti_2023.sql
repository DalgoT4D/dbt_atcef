{{ config(
  materialized='table',
  tags=["final","final_niti_2023", "niti_2023", "niti"]
) }}

SELECT
    m.farmer_id,
    m.farmer_name,
    m.state,
    m.taluka,
    m.village,
    m.dam,
    m.district,
    w.ngo_name,
    e.type_of_land_silt_is_spread_on,
    e.total_farm_area_silt_is_spread_on,
    e.distance_from_waterbody,
    CASE
        WHEN e.encounter_type = 'Farmer Endline' THEN 'Endline Done'
        ELSE 'Endline Not Done'
    END AS endline_status
FROM {{ ref('farmer_niti_2023') }} AS m
LEFT JOIN {{ ref('encounter_2023') }} AS e
    ON m.farmer_id = e.subject_id
LEFT JOIN
    {{ ref('work_order_niti_2023') }} AS w
    ON
        e.subject_id = w.work_order_id
