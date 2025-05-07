{{ config(
  materialized='table',
  tags=["final","final_gdgs_2025", "gdgs_2025", "gdgs"]
) }}


SELECT
    fc.work_order_name,
    fc.state,
    fc.district,
    fc.taluka,
    fc.dam,
    fc.village,
    fc.ngo_name,
    MAX(fc.date_time) AS date_time,
    MAX(fc.silt_target) AS silt_target,
    SUM(fc.total_silt_carted) AS silt_achieved,
    SUM(
        fe.total_farm_area_silt_is_spread_on
    ) AS total_farm_area_silt_is_spread_on
FROM {{ ref('farmer_calc_silt_gdgs_25') }} AS fc
INNER JOIN {{ ref('farmer_endline_gdgs_25') }} AS fe
    ON fc.farmer_id = fe.farmer_id
WHERE total_silt_carted::text != 'NaN'
GROUP BY
    fc.state,
    fc.district,
    fc.taluka,
    fc.dam,
    fc.village,
    fc.work_order_name,
    fc.ngo_name
