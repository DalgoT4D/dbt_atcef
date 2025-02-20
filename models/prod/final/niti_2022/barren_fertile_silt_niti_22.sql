{{ config(
  materialized='table',
  tags=["final","final_niti_2022", "niti_2022", "niti"]
) }}


    SELECT 
        fc.work_order_name,
        MAX(fc.date_time) as date_time,
        fc.state, 
        fc.district,
        fc.taluka,
        fc.dam,
        fc.ngo_name,
        fc.village,
        fe.endline_status,
        SUM(fc.total_silt_carted) as silt_achieved_by_endline_farmers,
        SUM(fe.total_farm_area_silt_is_spread_on) as total_farm_area_silt_is_spread_on,
        fe.type_of_land_silt_is_spread_on  
    FROM {{ref('farmer_calc_silt_niti_22')}} AS fc
    INNER JOIN {{ref('farmer_endline_niti_2022')}} AS fe
    on fc.farmer_id = fe.farmer_id
    WHERE total_silt_carted::text != 'NaN' AND endline_status = 'Endline Done' AND fc.total_silt_carted>0 and fe.total_farm_area_silt_is_spread_on>0
    GROUP BY
        fc.state, fc.district, fc.taluka, fc.dam, fc.village, fc.work_order_name, fc.ngo_name, fe.type_of_land_silt_is_spread_on, fe.endline_status


