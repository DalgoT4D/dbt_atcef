{{ config(
  materialized='table',
  tags=["final","final_gdgs_2023"]
) }}


    SELECT 
        work_order_name,
        MAX(date_time) as date_time,
        state, 
        district,
        taluka,
        dam,
        village,
        ngo_name,
        MAX(silt_target) as silt_target,
        SUM(total_silt_carted) as silt_achieved
    FROM {{ref('farmer_calc_silt_gdgs_23')}}
    WHERE total_silt_carted != 'NaN'
    GROUP BY
        state, district, taluka, dam, village, work_order_name, ngo_name


