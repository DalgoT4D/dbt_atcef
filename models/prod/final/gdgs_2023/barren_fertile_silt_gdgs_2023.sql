{{ config(
  materialized='table',
  tags=["final","final_gdgs_2023", "gdgs_2023", "gdgs"]
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
        fe.endline_status,
        SUM(fc.total_silt_carted) as silt_achieved_by_endline_farmers,
        SUM(fe.total_farm_area_silt_is_spread_on) as total_farm_area_silt_is_spread_on,
        fe.type_of_land_silt_is_spread_on
    FROM {{ref('farmer_calc_silt_gdgs_23')}} AS fc
    INNER JOIN {{ref('farmer_endline_gdgs_2023')}} AS fe
    ON fe.farmer_id = fc.farmer_id
    WHERE total_silt_carted != 'NaN' AND endline_status = 'Endline Done'
    GROUP BY
        fc.state, fc.district, fc.taluka, fc.dam, fc.village, fc.work_order_name, fc.ngo_name, fe.endline_status, fe.type_of_land_silt_is_spread_on


