{{ config(
  materialized='table',
  tags=["final","final_gdgs_2024", "gdgs_2024", "gdgs"]
) }}


    SELECT 
        fc.work_order_name,
        MAX(fc.date_time) as date_time,
        fc.state, 
        fc.district,
        fc.taluka,
        fc.dam,
        fc.village,
        fc.ngo_name,
        MAX(fc.silt_target) as silt_target,
        SUM(fc.total_silt_carted) as silt_achieved,
        SUM(fe.total_farm_area_silt_is_spread_on) as total_farm_area_silt_is_spread_on
    FROM {{ref('farmer_calc_silt_gdgs_24')}} AS fc
    INNER JOIN {{ref('farmer_endline')}} AS fe
    ON fe.farmer_id = fc.farmer_id
    WHERE total_silt_carted::text != 'NaN'
    GROUP BY
        fc.state, fc.district, fc.taluka, fc.dam, fc.village,fc.work_order_name, fc.ngo_name


