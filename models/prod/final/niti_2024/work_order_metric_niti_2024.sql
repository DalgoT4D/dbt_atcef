{{ config(
  materialized='table'
) }}


    SELECT 
        work_order_name,
        MAX(date_time) as date_time,
        state, 
        district,
        taluka,
        dam,
        village,
        MAX(silt_target) as silt_target,
        SUM(total_silt_carted) as silt_achieved
    FROM {{ref('farmer_calc_silt_niti_2024')}}
    GROUP BY
        state, district, taluka, dam, village, work_order_name


